package lima.data.predicate;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.dataset.Dataset;
import lima.data.domain.DomainSubSet;
import lima.data.domain.TPDomainSet;

/** Standalone smoke tests for OrderedPG - run directly, no test framework wired up yet. */
public final class OrderedPGTest {

    // rows: id=[0,1,2,3], score=[1.5,1.5,2.5,1.5]
    private static final String CSV =
        "id(Integer),score(Double)\n" +
        "0,1.5\n" +
        "1,1.5\n" +
        "2,2.5\n" +
        "3,1.5\n";

    // (0,1): id 0<1, score equal | (0,3): id 0<3, score equal
    // (2,0): id 2>0, score 2.5>1.5 | (1,1): id equal, score equal (self-pair)
    private static final int[][] PAIRS = {{0, 1}, {0, 3}, {2, 0}, {1, 1}};

    public static void main(String[] args) throws IOException {
        testPgEvalInteger();
        testPgEvalDouble();
        testPredPositionsAllPreds();
        testPredPositionsRestrictedPreds();
        testEvalFullDomain();
        testEvalWithSubsetDomain();
        testRejectsCategoryColumn();
        testNaNComparisons();
        System.out.println("All OrderedPG tests passed.");
    }

    private static void testPgEvalInteger() throws IOException {
        OrderedPG pg = buildPG("id(Integer)");
        TPDomainSet domain = new TPDomainSet(PAIRS);

        List<byte[]> variables = pg.pgEval(domain, null);
        check(variables.size() == 2, "OrderedPG should have two underlying variables (same, less)");
        check(Arrays.equals(variables.get(0), new byte[] {0, 0, 0, 1}), "id same should be [F,F,F,T]");
        check(Arrays.equals(variables.get(1), new byte[] {1, 1, 0, 0}), "id less should be [T,T,F,F]");
        System.out.println("testPgEvalInteger OK");
    }

    private static void testPgEvalDouble() throws IOException {
        OrderedPG pg = buildPG("score(Double)");
        TPDomainSet domain = new TPDomainSet(PAIRS);

        List<byte[]> variables = pg.pgEval(domain, null);
        check(Arrays.equals(variables.get(0), new byte[] {1, 1, 0, 1}), "score same should be [T,T,F,T]");
        check(Arrays.equals(variables.get(1), new byte[] {0, 0, 0, 0}), "score less should be all false");
        System.out.println("testPgEvalDouble OK");
    }

    private static void testPredPositionsAllPreds() throws IOException {
        OrderedPG pg = buildPG("id(Integer)");
        TPDomainSet domain = new TPDomainSet(PAIRS);
        List<byte[]> variables = pg.pgEval(domain, null);

        Map<Integer, int[]> positions = pg.predPositions(variables, null);
        check(positions.keySet().equals(Set.of(0, 1, 2, 3)), "null preds should compute all four predicates");
        check(Arrays.equals(positions.get(0), new int[] {3}), "pred 0 (equal) should be pair 3");
        check(Arrays.equals(positions.get(1), new int[] {0, 1, 2}), "pred 1 (not equal) should be pairs 0,1,2");
        check(Arrays.equals(positions.get(2), new int[] {0, 1}), "pred 2 (less) should be pairs 0,1");
        check(Arrays.equals(positions.get(3), new int[] {2}), "pred 3 (greater) should be pair 2");
        System.out.println("testPredPositionsAllPreds OK");
    }

    private static void testPredPositionsRestrictedPreds() throws IOException {
        OrderedPG pg = buildPG("score(Double)");
        TPDomainSet domain = new TPDomainSet(PAIRS);
        List<byte[]> variables = pg.pgEval(domain, null);

        Map<Integer, int[]> positions = pg.predPositions(variables, Set.of(0, 3));
        check(positions.keySet().equals(Set.of(0, 3)), "restricting preds should only compute the requested ones");
        check(Arrays.equals(positions.get(0), new int[] {0, 1, 3}), "pred 0 (equal) should be pairs 0,1,3");
        check(Arrays.equals(positions.get(3), new int[] {2}), "pred 3 (greater) should be pair 2");
        System.out.println("testPredPositionsRestrictedPreds OK");
    }

    private static void testEvalFullDomain() throws IOException {
        OrderedPG pg = buildPG("id(Integer)");
        TPDomainSet domain = new TPDomainSet(PAIRS);

        Map<Integer, DomainSubSet> result = pg.eval(domain, null, null);
        check(Arrays.equals(result.get(2).indices(), new int[] {0, 1}), "full-domain pred 2 (less) indices");
        check(Arrays.equals(result.get(3).indices(), new int[] {2}), "full-domain pred 3 (greater) indices");
        System.out.println("testEvalFullDomain OK");
    }

    private static void testEvalWithSubsetDomain() throws IOException {
        OrderedPG pg = buildPG("id(Integer)");
        TPDomainSet domain = new TPDomainSet(PAIRS);

        // exclude pair 0 ((0,1), one of the two "less" pairs)
        DomainSubSet x = DomainSubSet.make(4, new int[] {1, 2, 3});
        Map<Integer, DomainSubSet> result = pg.eval(domain, x, null);

        check(Arrays.equals(result.get(2).indices(), new int[] {1}),
            "pred 2 (less) should drop pair 0 once it's excluded, keeping pair 1");
        check(Arrays.equals(result.get(3).indices(), new int[] {2}),
            "pred 3 (greater) should be unaffected by excluding pair 0");
        System.out.println("testEvalWithSubsetDomain OK");
    }

    private static void testRejectsCategoryColumn() throws IOException {
        Path csv = writeTempCsv("label\na\nb\n");
        Dataset dataset = new Dataset(csv.toString(), 2);
        Files.deleteIfExists(csv);

        boolean threw = false;
        try {
            new OrderedPG(dataset, "label");
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        check(threw, "OrderedPG should reject a CATEGORY column");
        System.out.println("testRejectsCategoryColumn OK");
    }

    private static void testNaNComparisons() throws IOException {
        // row 0's score is blank -> NaN (Dataset's documented missing-value handling for DOUBLE).
        // A second column keeps the row from being an entirely blank line, which
        // Dataset intentionally skips (matching pandas' default skip_blank_lines).
        Path csv = writeTempCsv("score(Double),other\n,x\n1.5,y\n");
        Dataset dataset = new Dataset(csv.toString(), 2);
        Files.deleteIfExists(csv);

        OrderedPG pg = new OrderedPG(dataset, "score(Double)");
        TPDomainSet domain = new TPDomainSet(new int[][] {{0, 1}});

        List<byte[]> variables = pg.pgEval(domain, null);
        check(variables.get(0)[0] == 0, "NaN compared to anything should never be 'same'");
        check(variables.get(1)[0] == 0, "NaN compared to anything should never be 'less'");

        // IEEE 754 quirk inherited unchanged from the Python/numpy version:
        // greater is defined as not(same or less), so a NaN pair reads as
        // "greater" even though the comparison is really undefined.
        Map<Integer, int[]> positions = pg.predPositions(variables, null);
        check(positions.get(3).length == 1, "NaN pair is classified as 'greater', matching numpy's same quirk");

        System.out.println("testNaNComparisons OK");
    }

    private static OrderedPG buildPG(String column) throws IOException {
        Path csv = writeTempCsv(CSV);
        Dataset dataset = new Dataset(csv.toString(), 4);
        Files.deleteIfExists(csv);
        return new OrderedPG(dataset, column);
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("ordered-pg-test", ".csv");
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
