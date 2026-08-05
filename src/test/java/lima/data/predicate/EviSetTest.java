package lima.data.predicate;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import lima.data.dataset.Dataset;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;
import lima.data.domain.TPDomain;
import lima.data.domain.TPDomainSet;
import lima.util.random.RandZ2;

/** Standalone smoke tests for EviSet - run directly, no test framework wired up yet. */
public final class EviSetTest {

    // rows: id=[0,1,2,3,4,5], label=[a,b,a,b,a,b]
    private static final String CORE_CSV =
        "id(Integer),label\n" +
        "0,a\n" +
        "1,b\n" +
        "2,a\n" +
        "3,b\n" +
        "4,a\n" +
        "5,b\n";

    // hand-worked-out pairs (see PR discussion): 3 duplicate groups plus
    // singletons, all within a single 32-bit word (3 total bits: id.same,
    // id.less, label.same)
    private static final int[][] CORE_PAIRS = {
        {0, 1}, // p0: id less, label diff        -> pattern A
        {2, 3}, // p1: id less, label diff        -> pattern A (dup of p0)
        {4, 5}, // p2: id less, label diff        -> pattern A (dup of p0)
        {1, 0}, // p3: id greater, label diff     -> pattern B
        {0, 0}, // p4: id same, label same        -> pattern C
        {1, 1}, // p5: id same, label same        -> pattern C (dup of p4)
        {0, 2}, // p6: id less, label same        -> pattern D
        {3, 1}, // p7: id greater, label same     -> pattern E
    };

    public static void main(String[] args) throws IOException {
        testDeduplicatesAndCountsCorrectly();
        testTotalCountOnSubsetIndex();
        testFilterByPredicateMatchesHandComputedGroups();
        testChainedFilterByPredicateIntersectsCorrectly();
        testAllUniqueBehavioursNoCollapsing();
        testEmptyDomain();
        testMultiWordCrossCheckAgainstEval();
        System.out.println("All EviSet tests passed.");
    }

    private static void testDeduplicatesAndCountsCorrectly() throws IOException {
        EviSet eviSet = buildCoreEviSet();
        int[] full = eviSet.fullIndex();

        check(full.length == 5, "8 pairs collapsing into A(x3),B,C(x2),D,E should give 5 unique patterns");
        check(Arrays.equals(full, new int[] {0, 1, 2, 3, 4}), "fullIndex should be a plain 0..k-1 range");
        check(eviSet.totalCount(full) == 8, "counts should sum back up to the original 8 domain elements");
        System.out.println("testDeduplicatesAndCountsCorrectly OK");
    }

    private static void testTotalCountOnSubsetIndex() throws IOException {
        EviSet eviSet = buildCoreEviSet();
        // whichever unique pattern index corresponds to the size-3 duplicate group (A)
        int[] full = eviSet.fullIndex();
        int patternOfSizeThree = -1;
        for (int i : full) {
            if (eviSet.totalCount(new int[] {i}) == 3) {
                patternOfSizeThree = i;
            }
        }
        check(patternOfSizeThree >= 0, "one of the unique patterns should have count 3 (pattern A: p0,p1,p2)");
        check(eviSet.totalCount(new int[] {patternOfSizeThree}) == 3, "totalCount on a single-pattern index");
        System.out.println("testTotalCountOnSubsetIndex OK");
    }

    private static void testFilterByPredicateMatchesHandComputedGroups() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);
        List<PredicateGroup> pgs = PredicateGroup.buildPredicateGroups(dataset); // [id(Integer), label]

        TPDomainSet domain = new TPDomainSet(CORE_PAIRS);
        EviSet eviSet = new EviSet(pgs, domain, null);
        int[] full = eviSet.fullIndex();

        // id: pred0=equal -> p4,p5 (2) | pred2=less -> p0,p1,p2,p6 (4) | pred3=greater -> p3,p7 (2)
        check(eviSet.totalCount(eviSet.filterByPredicate(full, 0, 0)) == 2, "id equal count");
        check(eviSet.totalCount(eviSet.filterByPredicate(full, 0, 2)) == 4, "id less count");
        check(eviSet.totalCount(eviSet.filterByPredicate(full, 0, 3)) == 2, "id greater count");

        // label: pred0=same -> p4,p5,p6,p7 (4) | pred1=different -> p0,p1,p2,p3 (4)
        check(eviSet.totalCount(eviSet.filterByPredicate(full, 1, 0)) == 4, "label same count");
        check(eviSet.totalCount(eviSet.filterByPredicate(full, 1, 1)) == 4, "label different count");

        System.out.println("testFilterByPredicateMatchesHandComputedGroups OK");
    }

    private static void testChainedFilterByPredicateIntersectsCorrectly() throws IOException {
        EviSet eviSet = buildCoreEviSet();
        int[] full = eviSet.fullIndex();

        // id less (p0,p1,p2,p6) narrowed by label same (p4,p5,p6,p7) -> just p6
        int[] idLess = eviSet.filterByPredicate(full, 0, 2);
        int[] idLessAndLabelSame = eviSet.filterByPredicate(idLess, 1, 0);
        check(eviSet.totalCount(idLessAndLabelSame) == 1, "chained filter should intersect down to just pair p6");

        System.out.println("testChainedFilterByPredicateIntersectsCorrectly OK");
    }

    private static void testAllUniqueBehavioursNoCollapsing() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);
        List<PredicateGroup> pgs = PredicateGroup.buildPredicateGroups(dataset);

        // p0, p3, p4, p6, p7 from CORE_PAIRS are already mutually distinct
        int[][] distinctPairs = {{0, 1}, {1, 0}, {0, 0}, {0, 2}, {3, 1}};
        TPDomainSet domain = new TPDomainSet(distinctPairs);
        EviSet eviSet = new EviSet(pgs, domain, null);

        check(eviSet.fullIndex().length == 5, "5 already-distinct behaviours should not collapse at all");
        check(eviSet.totalCount(eviSet.fullIndex()) == 5, "counts should still sum to 5");
        System.out.println("testAllUniqueBehavioursNoCollapsing OK");
    }

    private static void testEmptyDomain() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);
        List<PredicateGroup> pgs = PredicateGroup.buildPredicateGroups(dataset);

        TPDomainSet domain = new TPDomainSet(CORE_PAIRS); // size 8
        DomainSubSet empty = DomainSubSet.make(8, new int[0]);

        EviSet eviSet = new EviSet(pgs, domain, empty);
        check(eviSet.fullIndex().length == 0, "an empty domain subset should produce zero unique patterns");
        check(eviSet.totalCount(eviSet.fullIndex()) == 0, "totalCount over an empty index should be 0");
        System.out.println("testEmptyDomain OK");
    }

    /**
     * Broader, automatic cross-check: build a dataset wide enough that the
     * flattened bit count exceeds 32 (forcing the multi-word packing path),
     * generate a batch of random pairs, and verify that for every predicate
     * of every group, EviSet's deduplicated totalCount/filterByPredicate
     * recovers exactly the same count as the already-tested, non-deduplicated
     * PredicateGroup.eval() path over the same domain.
     */
    private static void testMultiWordCrossCheckAgainstEval() throws IOException {
        int numColumns = 20; // 20 OrderedPG columns * 2 bits = 40 bits -> 2 words
        int numRows = 12;
        StringBuilder header = new StringBuilder();
        for (int c = 0; c < numColumns; c++) {
            if (c > 0) {
                header.append(',');
            }
            header.append("col").append(c).append("(Integer)");
        }
        StringBuilder content = new StringBuilder(header).append('\n');
        Random valueRandom = new Random(99);
        for (int r = 0; r < numRows; r++) {
            for (int c = 0; c < numColumns; c++) {
                if (c > 0) {
                    content.append(',');
                }
                content.append(valueRandom.nextInt(4)); // small range -> guarantees real duplication too
            }
            content.append('\n');
        }
        Path csv = writeTempCsv(content.toString());
        Dataset dataset = new Dataset(csv.toString(), numRows);
        Files.deleteIfExists(csv);

        List<PredicateGroup> pgs = PredicateGroup.buildPredicateGroups(dataset);
        check(pgs.size() == numColumns, "sanity: one OrderedPG per column");

        TPDomain tpDomain = new TPDomain(numRows, new RandZ2(64, new Random(17)));
        TPDomainSet domainSet = tpDomain.makeDomainSet(100);
        DomainSet<int[]> domain = domainSet;

        EviSet eviSet = new EviSet(pgs, domain, null);
        int[] full = eviSet.fullIndex();
        check(eviSet.totalCount(full) == 100, "deduplicated counts should still sum to the sampled domain size");

        for (int pgIdx = 0; pgIdx < pgs.size(); pgIdx++) {
            PredicateGroup pg = pgs.get(pgIdx);
            for (int predIdx = 0; predIdx < pg.numPredicates(); predIdx++) {
                int expected = pg.eval(domain, null, null).get(predIdx).size();
                int actual = eviSet.totalCount(eviSet.filterByPredicate(full, pgIdx, predIdx));
                check(expected == actual,
                    "pg " + pgIdx + " pred " + predIdx + ": expected " + expected + " from eval(), got " + actual);
            }
        }
        System.out.println("testMultiWordCrossCheckAgainstEval OK");
    }

    private static EviSet buildCoreEviSet() throws IOException {
        Path csv = writeTempCsv(CORE_CSV);
        Dataset dataset = new Dataset(csv.toString(), 6);
        Files.deleteIfExists(csv);
        List<PredicateGroup> pgs = PredicateGroup.buildPredicateGroups(dataset);
        TPDomainSet domain = new TPDomainSet(CORE_PAIRS);
        return new EviSet(pgs, domain, null);
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("eviset-test", ".csv");
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
