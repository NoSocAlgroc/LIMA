package lima.sampler;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import lima.data.dataset.Dataset;
import lima.data.domain.Domain;
import lima.data.domain.TPDifferenceDomain;
import lima.data.domain.TPDomainSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;
import lima.lattice.PredicateRef;
import lima.scheduler.Schedule;
import lima.scheduler.TopUpScheduler;
import lima.util.random.RandZ2;

/** Standalone smoke tests for TopUpSampler - run directly, no test framework wired up yet. */
public final class TopUpSamplerTest {

    // same hand-verified scenario as SampleTest/EviSetTest: id less = 4/8,
    // label same = 4/8, root always sees all 8 (a=8,b=0) since it has no filter.
    private static final String CORE_CSV = "id(Integer),label\n0,a\n1,b\n2,a\n3,b\n4,a\n5,b\n";
    private static final int[][] CORE_PAIRS = {
        {0, 1}, {2, 3}, {4, 5}, {1, 0}, {0, 0}, {1, 1}, {0, 2}, {3, 1},
    };

    public static void main(String[] args) throws IOException {
        testOvershootScalesDownToExactRemainingNeed();
        testExactMatchPassesThroughUnscaled();
        testWellUnderRemainingNeedPassesThroughUnscaled();
        testAlreadyPastTargetScalesToZero();
        testNodesNotInTargetsPassThroughUnscaled();
        testZeroBatchSizeProducesNoCrash();
        testConvergesToExactTargetsOverMultipleRounds();
        System.out.println("All TopUpSampler tests passed.");
    }

    private static void testOvershootScalesDownToExactRemainingNeed() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        // root target=10 (remaining=8, matches raw exactly - see next test);
        // idLess target=4 (currentTotal defaults to 2 -> remaining=2, raw=(4,4) totals 8 -> overshoots)
        Map<Lattice.Node, Long> targets = new HashMap<>();
        targets.put(setup.lattice.getRoot(), 10L);
        targets.put(idLessNode(setup), 4L);

        TopUpSampler sampler = new TopUpSampler(targets);
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 8);

        long[] ab = result.counts.get(idLessNode(setup));
        check(ab[0] + ab[1] == 2, "idLess should be scaled down to exactly its remaining need of 2, got " + (ab[0] + ab[1]));
        check(ab[0] == 1 && ab[1] == 1, "scaling (4,4) down to sum 2 should preserve the 1:1 ratio, got (" + ab[0] + "," + ab[1] + ")");
        System.out.println("testOvershootScalesDownToExactRemainingNeed OK");
    }

    private static void testExactMatchPassesThroughUnscaled() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        Map<Lattice.Node, Long> targets = new HashMap<>();
        targets.put(setup.lattice.getRoot(), 10L); // currentTotal=2 -> remaining=8, raw total is exactly 8

        TopUpSampler sampler = new TopUpSampler(targets);
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 8);

        long[] ab = result.counts.get(setup.lattice.getRoot());
        check(ab[0] == 8 && ab[1] == 0, "root's raw (8,0) exactly matches its remaining need, so it should pass through unscaled");
        System.out.println("testExactMatchPassesThroughUnscaled OK");
    }

    private static void testWellUnderRemainingNeedPassesThroughUnscaled() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        Map<Lattice.Node, Long> targets = new HashMap<>();
        targets.put(setup.lattice.getRoot(), 1000L); // remaining=998, raw total=8, nowhere near overshooting

        TopUpSampler sampler = new TopUpSampler(targets);
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 8);

        long[] ab = result.counts.get(setup.lattice.getRoot());
        check(ab[0] == 8 && ab[1] == 0, "well under the remaining need, root should pass through unscaled");
        System.out.println("testWellUnderRemainingNeedPassesThroughUnscaled OK");
    }

    private static void testAlreadyPastTargetScalesToZero() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        // root's currentTotal (2, from the seeded prior) already exceeds this tiny target
        Map<Lattice.Node, Long> targets = new HashMap<>();
        targets.put(setup.lattice.getRoot(), 1L);

        TopUpSampler sampler = new TopUpSampler(targets);
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 8);

        long[] ab = result.counts.get(setup.lattice.getRoot());
        check(ab[0] == 0 && ab[1] == 0, "a target already met before this round should clamp remaining need to 0, got (" + ab[0] + "," + ab[1] + ")");
        System.out.println("testAlreadyPastTargetScalesToZero OK");
    }

    private static void testNodesNotInTargetsPassThroughUnscaled() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        TopUpSampler sampler = new TopUpSampler(Map.of()); // nothing has a target
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 8);

        long[] rootAb = result.counts.get(setup.lattice.getRoot());
        long[] idLessAb = result.counts.get(idLessNode(setup));
        check(rootAb[0] == 8 && rootAb[1] == 0, "root has no target, so it should pass through unscaled");
        check(idLessAb[0] == 4 && idLessAb[1] == 4, "idLess has no target, so it should pass through unscaled");
        System.out.println("testNodesNotInTargetsPassThroughUnscaled OK");
    }

    private static void testZeroBatchSizeProducesNoCrash() throws IOException {
        Setup setup = buildCoreSetup();
        Schedule sched = rootAndIdLessSchedule(setup.lattice);

        Map<Lattice.Node, Long> targets = new HashMap<>();
        targets.put(setup.lattice.getRoot(), 10L);

        TopUpSampler sampler = new TopUpSampler(targets);
        Sample result = sampler.sample(sched, setup.domain, setup.predicateGroups, setup.state, 0);

        long[] ab = result.counts.get(setup.lattice.getRoot());
        check(ab[0] == 0 && ab[1] == 0, "a zero-size batch should produce (0,0) everywhere without dividing by zero");
        System.out.println("testZeroBatchSizeProducesNoCrash OK");
    }

    /**
     * Full loop, mirroring how LIMA.step() would drive this phase: schedule
     * -> sample -> update, repeated until the schedule is empty, drawing
     * from a real TPDifferenceDomain. Verifies every targeted node's final
     * accumulated total lands EXACTLY on its precomputed target - not just
     * under or near it.
     */
    private static void testConvergesToExactTargetsOverMultipleRounds() throws IOException {
        int oldRange = 6;
        int newRange = 12;
        StringBuilder csv = new StringBuilder("id(Integer),label\n");
        for (int i = 0; i < newRange; i++) {
            csv.append(i).append(',').append(i % 2 == 0 ? "a" : "b").append('\n');
        }
        Path path = writeTempCsv(csv.toString());
        Dataset dataset = new Dataset(path.toString(), newRange);
        Files.deleteIfExists(path);

        List<PredicateGroup> predicateGroups = PredicateGroup.buildPredicateGroups(dataset);
        int[] pgs = new int[predicateGroups.size()];
        for (int i = 0; i < pgs.length; i++) {
            pgs[i] = predicateGroups.get(i).numPredicates();
        }
        Lattice lattice = new Lattice(pgs);
        LIMALatticeState state = new LIMALatticeState(lattice);

        // simulate "stage 1 already ran": seed a couple of nodes with evidence
        Lattice.Node root = lattice.getRoot();
        Lattice.Node idLess = root.to(0, 2).to();
        state.updateNode(root, 8, 0);   // x = 1+8+1+0 = 10
        state.updateNode(idLess, 3, 3); // x = 1+3+1+3 = 8

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, oldRange, newRange); // scale = 144/36 = 4
        check(targets.get(root) == 40L, "sanity: root target should be 10*4=40");
        check(targets.get(idLess) == 32L, "sanity: idLess target should be 8*4=32");

        TopUpScheduler scheduler = new TopUpScheduler(targets);
        TopUpSampler sampler = new TopUpSampler(targets);
        Domain<TPDomainSet> domain = new TPDifferenceDomain(oldRange, newRange, new RandZ2(64, new Random(123)));

        for (int i = 0; i < 500; i++) {
            Schedule sched = scheduler.schedule(state, 0.0);
            if (sched.parents.isEmpty()) {
                break;
            }
            Sample result = sampler.sample(sched, domain, predicateGroups, state, 10);
            state.update(result.counts, 4.0);
        }

        for (Map.Entry<Lattice.Node, Long> entry : targets.entrySet()) {
            long finalTotal = state.getDist(entry.getKey()).a + state.getDist(entry.getKey()).b;
            check(finalTotal == entry.getValue(),
                "node " + entry.getKey() + " should land exactly on its target " + entry.getValue() + ", got " + finalTotal);
        }
        System.out.println("testConvergesToExactTargetsOverMultipleRounds OK");
    }

    // ------------------------------------------------------------------

    private static final class Setup {
        Lattice lattice;
        LIMALatticeState state;
        List<PredicateGroup> predicateGroups;
        Domain<TPDomainSet> domain;
    }

    private static Lattice.Node idLessNode(Setup setup) {
        return setup.lattice.getRoot().to(0, 2).to();
    }

    private static Schedule rootAndIdLessSchedule(Lattice lattice) {
        Map<Lattice.Node, Set<PredicateRef>> children = new HashMap<>();
        children.put(lattice.getRoot(), Set.of(new PredicateRef(0, 2)));
        return new Schedule(new HashMap<>(), children);
    }

    private static Setup buildCoreSetup() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);

        Setup setup = new Setup();
        setup.predicateGroups = PredicateGroup.buildPredicateGroups(dataset); // [id(Integer), label]
        setup.lattice = new Lattice(new int[] {4, 2});
        setup.state = new LIMALatticeState(setup.lattice);
        // a fixed-pairs stand-in domain, but still honoring the requested
        // size (0 in particular, for testZeroBatchSizeProducesNoCrash)
        setup.domain = size -> new TPDomainSet(java.util.Arrays.copyOfRange(CORE_PAIRS, 0, Math.min(size, CORE_PAIRS.length)));
        return setup;
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("topup-sampler-test", ".csv");
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(content);
        }
        return path;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
