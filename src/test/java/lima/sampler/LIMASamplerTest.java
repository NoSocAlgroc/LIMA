package lima.sampler;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Random;

import lima.data.dataset.Dataset;
import lima.data.domain.DomainSubSet;
import lima.data.domain.TPDomain;
import lima.data.domain.TPDomainSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;
import lima.lattice.PredicateRef;
import lima.scheduler.LIMAScheduler;
import lima.scheduler.Schedule;
import lima.util.random.RandZ2;

/** Standalone smoke tests for LIMASampler - run directly, no test framework wired up yet. */
public final class LIMASamplerTest {

    private static final String DATASET_CSV = buildDatasetCsv();

    public static void main(String[] args) throws IOException {
        testRootAlwaysAccountsForAllNSamples();
        testChildCountsNeverExceedParent();
        testDepth1CountsMatchIndependentEval();
        System.out.println("All LIMASampler tests passed.");
    }

    private static void testRootAlwaysAccountsForAllNSamples() throws IOException {
        Fixture f = buildFixture(0.0, new Random(11));
        int n = 500;

        Sample result = f.sampler.sample(f.schedule, f.domain, f.predicateGroups, f.state, n);

        long[] rootCounts = result.counts.get(f.lattice.getRoot());
        check(rootCounts != null, "the root should always be visited");
        check(rootCounts[0] + rootCounts[1] == n, "root's a+b should always equal n, got " + (rootCounts[0] + rootCounts[1]));
        check(rootCounts[1] == 0, "root's b should be 0 - the full domain always satisfies the (empty) root predicate set");
        System.out.println("testRootAlwaysAccountsForAllNSamples OK");
    }

    private static void testChildCountsNeverExceedParent() throws IOException {
        Fixture f = buildFixture(0.0, new Random(22));
        int n = 500;

        Sample result = f.sampler.sample(f.schedule, f.domain, f.predicateGroups, f.state, n);

        for (Map.Entry<Lattice.Node, java.util.Set<PredicateRef>> entry : f.schedule.children.entrySet()) {
            Lattice.Node parent = entry.getKey();
            long[] parentCounts = result.counts.get(parent);
            if (parentCounts == null) {
                continue;
            }
            long parentTotal = parentCounts[0] + parentCounts[1];

            for (PredicateRef ref : entry.getValue()) {
                Lattice.Node child = parent.to(ref.pg(), ref.p()).to();
                long[] childCounts = result.counts.get(child);
                check(childCounts != null, "every scheduled child should have been visited");
                long childTotal = childCounts[0] + childCounts[1];
                check(childTotal <= parentTotal,
                    "a child's total (" + childTotal + ") should never exceed its parent's (" + parentTotal + ")");
                check(childCounts[0] >= 0 && childCounts[1] >= 0, "counts should never be negative");
            }
        }
        System.out.println("testChildCountsNeverExceedParent OK");
    }

    private static void testDepth1CountsMatchIndependentEval() throws IOException {
        long seed = 777;
        int n = 500;
        int range = 20;

        Fixture f = buildFixture(0.0, new Random(seed));
        Sample result = f.sampler.sample(f.schedule, new TPDomain(range, new RandZ2(64, new Random(seed))), f.predicateGroups, f.state, n);

        // an independently, identically-seeded RandZ2 draws the exact same
        // pairs LIMASampler's internal domain draw would have, so depth-1
        // counts can be cross-checked against a direct PredicateGroup.eval()
        TPDomainSet domainSet = new TPDomain(range, new RandZ2(64, new Random(seed))).makeDomainSet(n);
        DomainSubSet full = domainSet.full();

        Lattice.Node root = f.lattice.getRoot();
        java.util.Set<PredicateRef> rootChildren = f.schedule.children.getOrDefault(root, java.util.Set.of());
        check(!rootChildren.isEmpty(), "sanity: the root should have at least one scheduled child to check");

        for (PredicateRef ref : rootChildren) {
            Lattice.Node child = root.to(ref.pg(), ref.p()).to();
            long expected = f.predicateGroups.get(ref.pg()).eval(domainSet, full, java.util.Set.of(ref.p())).get(ref.p()).size();
            long[] actual = result.counts.get(child);
            check(actual != null, "scheduled child " + child + " should have been visited");
            check(actual[0] == expected,
                "pg " + ref.pg() + " pred " + ref.p() + ": expected " + expected + " from independent eval(), got " + actual[0]);
            check(actual[1] == n - expected, "b should be n - a");
        }
        System.out.println("testDepth1CountsMatchIndependentEval OK");
    }

    // ------------------------------------------------------------------

    private static final class Fixture {
        Lattice lattice;
        LIMALatticeState state;
        List<PredicateGroup> predicateGroups;
        TPDomain domain;
        Schedule schedule;
        LIMASampler sampler = new LIMASampler();
    }

    private static Fixture buildFixture(double threshold, Random random) throws IOException {
        Path csv = writeTempCsv(DATASET_CSV);
        Dataset dataset = new Dataset(csv.toString(), 20);
        Files.deleteIfExists(csv);

        Fixture f = new Fixture();
        f.predicateGroups = PredicateGroup.buildPredicateGroups(dataset); // [id(Integer), score(Double), label]
        int[] pgs = new int[f.predicateGroups.size()];
        for (int i = 0; i < pgs.length; i++) {
            pgs[i] = f.predicateGroups.get(i).numPredicates();
        }
        f.lattice = new Lattice(pgs);
        f.state = new LIMALatticeState(f.lattice);
        f.schedule = new LIMAScheduler().schedule(f.state, threshold);
        f.domain = new TPDomain(20, new RandZ2(64, random));
        return f;
    }

    private static String buildDatasetCsv() {
        StringBuilder sb = new StringBuilder("id(Integer),score(Double),label\n");
        for (int i = 0; i < 20; i++) {
            sb.append(i).append(',').append(i % 5).append(',').append(i % 3 == 0 ? "a" : "b").append('\n');
        }
        return sb.toString();
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("lima-sampler-test", ".csv");
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
