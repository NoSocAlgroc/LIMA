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
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;
import lima.data.domain.TPDomain;
import lima.data.domain.TPDomainSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;
import lima.lattice.PredicateRef;
import lima.scheduler.Schedule;
import lima.util.random.RandZ2;

/** Standalone smoke tests for Sample's static helpers - run directly, no test framework wired up yet. */
public final class SampleTest {

    // rows: id=[0,1,2,3,4,5], label=[a,b,a,b,a,b] - same scenario already
    // hand-verified in EviSetTest, reused here so the expected counts are
    // already known-correct.
    private static final String CORE_CSV =
        "id(Integer),label\n" +
        "0,a\n1,b\n2,a\n3,b\n4,a\n5,b\n";

    private static final int[][] CORE_PAIRS = {
        {0, 1}, {2, 3}, {4, 5}, // id less, label diff
        {1, 0},                 // id greater, label diff
        {0, 0}, {1, 1},         // id same, label same
        {0, 2},                 // id less, label same
        {3, 1},                 // id greater, label same
    };

    public static void main(String[] args) throws IOException {
        testMakeFullDomainSet();
        testEstimateReachPropagatesThroughTree();
        testPartitionPredicateGroupsSplitsByThreshold();
        testPartitionPredicateGroupsDefaultFactor();
        testEvaluatePredicateGroupsOrdersByPredicateId();
        testEvaluatePredicateMatchesFullEval();
        testFilterDomainUsesPrecomputedWhenAvailable();
        testFilterDomainFallsBackWhenNotPrecomputed();
        testExploreMatchesHandComputedCounts_precomputed();
        testExploreMatchesHandComputedCounts_onDemand();
        System.out.println("All Sample tests passed.");
    }

    private static void testMakeFullDomainSet() {
        Domain<TPDomainSet> domain = new TPDomain(50, new RandZ2(64, new Random(1)));
        Map.Entry<DomainSet<int[]>, DomainSubSet> result = Sample.makeFullDomainSet(domain, 30);

        check(result.getKey().size() == 30, "the produced DomainSet should have exactly n elements");
        check(result.getValue().size() == 30, "the full DomainSubSet should cover every position");
        check(java.util.Arrays.equals(result.getValue().indices(), DomainSubSet.full(30).indices()),
            "full should be indistinguishable from DomainSubSet.full(n)");
        System.out.println("testMakeFullDomainSet OK");
    }

    private static void testEstimateReachPropagatesThroughTree() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node root = lattice.getRoot();
        Lattice.Node a = root.to(0, 0).to();
        Lattice.Node ab = a.to(1, 0).to();

        state.updateNode(a, 1, 2); // a=2, b=3 -> mean = 0.4 (root stays default: a=1,b=1 -> mean=0.5)

        Map<Lattice.Node, Set<PredicateRef>> children = new HashMap<>();
        children.put(root, Set.of(new PredicateRef(0, 0)));
        children.put(a, Set.of(new PredicateRef(1, 0)));
        Schedule sched = new Schedule(new HashMap<>(), children);

        double[] counts = Sample.estimateReach(sched, state, 1000, 2);

        // root -> A: 1000 * 0.5 = 500 into counts[0]
        // A -> AB: 500 * 0.4 = 200 into counts[1]
        check(Math.abs(counts[0] - 500.0) < 1e-9, "counts[0] should be 1000*0.5=500, got " + counts[0]);
        check(Math.abs(counts[1] - 200.0) < 1e-9, "counts[1] should be 500*0.4=200, got " + counts[1]);
        System.out.println("testEstimateReachPropagatesThroughTree OK");
    }

    private static void testPartitionPredicateGroupsSplitsByThreshold() {
        double[] counts = {500.0, 200.0, 5.0};
        Map.Entry<List<Integer>, List<Integer>> result = Sample.partitionPredicateGroups(counts, 1000, 0.01);
        // threshold = 1000*0.01 = 10
        check(result.getKey().equals(List.of(0, 1)), "pg 0 and 1 (500, 200 > 10) should be precomputed");
        check(result.getValue().equals(List.of(2)), "pg 2 (5 <= 10) should be on-demand");
        System.out.println("testPartitionPredicateGroupsSplitsByThreshold OK");
    }

    private static void testPartitionPredicateGroupsDefaultFactor() {
        double[] counts = {500.0, 200.0, 5.0};
        Map.Entry<List<Integer>, List<Integer>> withDefault = Sample.partitionPredicateGroups(counts, 1000);
        Map.Entry<List<Integer>, List<Integer>> explicit = Sample.partitionPredicateGroups(counts, 1000, 0.01);
        check(withDefault.getKey().equals(explicit.getKey()) && withDefault.getValue().equals(explicit.getValue()),
            "the default-factor overload should behave like factor=0.01");
        System.out.println("testPartitionPredicateGroupsDefaultFactor OK");
    }

    private static void testEvaluatePredicateGroupsOrdersByPredicateId() throws IOException {
        Setup setup = buildCoreSetup();

        Map<Integer, PredicateGroup> pgs = new HashMap<>();
        pgs.put(0, setup.predicateGroups.get(0));
        pgs.put(1, setup.predicateGroups.get(1));
        Map<Integer, List<DomainSubSet>> result = Sample.evaluatePredicateGroups(pgs, setup.domain, setup.full);

        check(result.get(0).size() == 4, "id (OrderedPG) should contribute 4 ordered predicates");
        check(result.get(1).size() == 2, "label (UnorderedPG) should contribute 2 ordered predicates");

        Map<Integer, DomainSubSet> idDirect = setup.predicateGroups.get(0).eval(setup.domain, setup.full, null);
        for (int p = 0; p < 4; p++) {
            check(java.util.Arrays.equals(result.get(0).get(p).indices(), idDirect.get(p).indices()),
                "evaluatePredicateGroups' pred " + p + " should match a direct eval() call");
        }
        System.out.println("testEvaluatePredicateGroupsOrdersByPredicateId OK");
    }

    private static void testEvaluatePredicateMatchesFullEval() throws IOException {
        Setup setup = buildCoreSetup();
        PredicateGroup idPg = setup.predicateGroups.get(0);

        DomainSubSet viaSingle = Sample.evaluatePredicate(idPg, 2, setup.domain, setup.full);
        DomainSubSet viaFull = idPg.eval(setup.domain, setup.full, null).get(2);

        check(java.util.Arrays.equals(viaSingle.indices(), viaFull.indices()),
            "evaluating just predicate 2 should match that same predicate's entry from a full eval()");
        System.out.println("testEvaluatePredicateMatchesFullEval OK");
    }

    private static void testFilterDomainUsesPrecomputedWhenAvailable() throws IOException {
        Setup setup = buildCoreSetup();
        PredicateGroup labelPg = setup.predicateGroups.get(1);

        // deliberately wrong "precomputed" entry (always-empty) for pg 1's
        // predicate 0, standing in for whatever a real precompute pass
        // would have produced - if filterDomain actually trusts this value
        // instead of recomputing the (different, non-empty) real answer,
        // the result must come out empty too.
        DomainSubSet trap = DomainSubSet.make(setup.domain.size(), new int[0]);
        Map<Integer, List<DomainSubSet>> precomputed = new HashMap<>();
        precomputed.put(1, List.of(trap, trap));

        DomainSubSet result = Sample.filterDomain(setup.full, 1, labelPg, 0, setup.domain, precomputed);
        check(result.size() == 0, "filterDomain should have used the injected (empty) precomputed value, not recomputed the real answer");
        System.out.println("testFilterDomainUsesPrecomputedWhenAvailable OK");
    }

    private static void testFilterDomainFallsBackWhenNotPrecomputed() throws IOException {
        Setup setup = buildCoreSetup();
        PredicateGroup idPg = setup.predicateGroups.get(0);

        DomainSubSet result = Sample.filterDomain(setup.full, 0, idPg, 2, setup.domain, Map.of());
        DomainSubSet direct = Sample.evaluatePredicate(idPg, 2, setup.domain, setup.full);

        check(java.util.Arrays.equals(result.indices(), direct.indices()),
            "with no precomputed entry, filterDomain should fall back to evaluating the predicate directly");
        System.out.println("testFilterDomainFallsBackWhenNotPrecomputed OK");
    }

    private static void testExploreMatchesHandComputedCounts_precomputed() throws IOException {
        runExploreScenario(true);
        System.out.println("testExploreMatchesHandComputedCounts_precomputed OK");
    }

    private static void testExploreMatchesHandComputedCounts_onDemand() throws IOException {
        runExploreScenario(false);
        System.out.println("testExploreMatchesHandComputedCounts_onDemand OK");
    }

    /**
     * root -(id less)-> idLess -(label same)-> idLessLabelSame
     * root -(label same)-> labelSame
     *
     * Expected counts, hand-verified against the same CORE_PAIRS scenario
     * in EviSetTest: id less = 4/8, label same = 4/8, (id less AND label
     * same) = 1/8.
     */
    private static void runExploreScenario(boolean usePrecomputed) throws IOException {
        Setup setup = buildCoreSetup();
        Lattice.Node root = setup.lattice.getRoot();
        Lattice.Node idLess = root.to(0, 2).to();
        Lattice.Node labelSame = root.to(1, 0).to();
        Lattice.Node idLessLabelSame = idLess.to(1, 0).to();

        Map<Lattice.Node, Set<PredicateRef>> children = new HashMap<>();
        children.put(root, Set.of(new PredicateRef(0, 2), new PredicateRef(1, 0)));
        children.put(idLess, Set.of(new PredicateRef(1, 0)));
        Schedule sched = new Schedule(new HashMap<>(), children);

        Map<Integer, List<DomainSubSet>> precomputed;
        if (usePrecomputed) {
            Map<Integer, PredicateGroup> pgs = new HashMap<>();
            pgs.put(0, setup.predicateGroups.get(0));
            pgs.put(1, setup.predicateGroups.get(1));
            precomputed = Sample.evaluatePredicateGroups(pgs, setup.domain, setup.full);
        } else {
            precomputed = Map.of();
        }

        Sample result = Sample.explore(setup.domain, setup.full, sched, setup.state, setup.predicateGroups, precomputed);

        checkCounts(result, root, 8, 0);
        checkCounts(result, idLess, 4, 4);
        checkCounts(result, labelSame, 4, 4);
        checkCounts(result, idLessLabelSame, 1, 7);
    }

    private static void checkCounts(Sample result, Lattice.Node node, long expectedA, long expectedB) {
        long[] ab = result.counts.get(node);
        check(ab != null, "node " + node + " should have been visited");
        check(ab[0] == expectedA && ab[1] == expectedB,
            "node " + node + ": expected (" + expectedA + "," + expectedB + "), got (" + ab[0] + "," + ab[1] + ")");
    }

    private static final class Setup {
        Lattice lattice;
        LIMALatticeState state;
        List<PredicateGroup> predicateGroups;
        DomainSet<int[]> domain;
        DomainSubSet full;
    }

    private static Setup buildCoreSetup() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);

        Setup setup = new Setup();
        setup.predicateGroups = PredicateGroup.buildPredicateGroups(dataset); // [id(Integer), label]
        setup.lattice = new Lattice(new int[] {4, 2});
        setup.state = new LIMALatticeState(setup.lattice);
        TPDomainSet domainSet = new TPDomainSet(CORE_PAIRS);
        setup.domain = domainSet;
        setup.full = domainSet.full();
        return setup;
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("sample-test", ".csv");
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
