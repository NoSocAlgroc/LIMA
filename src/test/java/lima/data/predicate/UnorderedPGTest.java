package lima.data.predicate;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.dataset.Dataset;
import lima.data.domain.DomainSubSet;
import lima.data.domain.TPDomainSet;

/** Standalone smoke tests for UnorderedPG - run directly, no test framework wired up yet. */
public final class UnorderedPGTest {

    // rows: 0=a, 1=b, 2=a, 3=a
    private static final String CSV =
        "label\n" +
        "a\n" +
        "b\n" +
        "a\n" +
        "a\n";

    // (0,1): a/b -> diff | (0,3): a/a -> same | (2,0): a/a -> same | (1,1): b/b -> same
    private static final int[][] PAIRS = {{0, 1}, {0, 3}, {2, 0}, {1, 1}};

    public static void main(String[] args) throws IOException {
        testPgEval();
        testPredPositionsAllPreds();
        testPredPositionsRestrictedPreds();
        testEvalFullDomain();
        testEvalWithSubsetDomain();
        System.out.println("All UnorderedPG tests passed.");
    }

    private static void testPgEval() throws IOException {
        UnorderedPG pg = buildPG();
        TPDomainSet domain = new TPDomainSet(PAIRS);

        List<byte[]> variables = pg.pgEval(domain, null);
        check(variables.size() == 1, "UnorderedPG should have exactly one underlying variable (same)");
        check(java.util.Arrays.equals(variables.get(0), new byte[] {0, 1, 1, 1}),
            "same should be [diff, same, same, same]");
        System.out.println("testPgEval OK");
    }

    private static void testPredPositionsAllPreds() throws IOException {
        UnorderedPG pg = buildPG();
        TPDomainSet domain = new TPDomainSet(PAIRS);
        List<byte[]> variables = pg.pgEval(domain, null);

        Map<Integer, int[]> positions = pg.predPositions(variables, null);
        check(positions.keySet().equals(Set.of(0, 1)), "null preds should compute both predicates");
        check(java.util.Arrays.equals(positions.get(0), new int[] {1, 2, 3}), "pred 0 (same) should be rows 1,2,3");
        check(java.util.Arrays.equals(positions.get(1), new int[] {0}), "pred 1 (different) should be row 0");
        System.out.println("testPredPositionsAllPreds OK");
    }

    private static void testPredPositionsRestrictedPreds() throws IOException {
        UnorderedPG pg = buildPG();
        TPDomainSet domain = new TPDomainSet(PAIRS);
        List<byte[]> variables = pg.pgEval(domain, null);

        Map<Integer, int[]> positions = pg.predPositions(variables, Set.of(1));
        check(positions.keySet().equals(Set.of(1)), "restricting preds should only compute the requested ones");
        check(java.util.Arrays.equals(positions.get(1), new int[] {0}), "pred 1 (different) should still be row 0");
        System.out.println("testPredPositionsRestrictedPreds OK");
    }

    private static void testEvalFullDomain() throws IOException {
        UnorderedPG pg = buildPG();
        TPDomainSet domain = new TPDomainSet(PAIRS);

        Map<Integer, DomainSubSet> result = pg.eval(domain, null, null);
        check(java.util.Arrays.equals(result.get(0).indices(), new int[] {1, 2, 3}), "full-domain pred 0 indices");
        check(java.util.Arrays.equals(result.get(1).indices(), new int[] {0}), "full-domain pred 1 indices");
        System.out.println("testEvalFullDomain OK");
    }

    private static void testEvalWithSubsetDomain() throws IOException {
        UnorderedPG pg = buildPG();
        TPDomainSet domain = new TPDomainSet(PAIRS);

        // exclude pair 0 ((0,1), the only "different" pair)
        DomainSubSet x = DomainSubSet.make(4, new int[] {1, 2, 3});
        Map<Integer, DomainSubSet> result = pg.eval(domain, x, null);

        check(java.util.Arrays.equals(result.get(0).indices(), new int[] {1, 2, 3}),
            "pred 0 (same) should be unaffected by excluding the only differing pair");
        check(result.get(1).indices().length == 0,
            "pred 1 (different) should be empty once its only pair is excluded");
        System.out.println("testEvalWithSubsetDomain OK");
    }

    private static UnorderedPG buildPG() throws IOException {
        Path csv = writeTempCsv(CSV);
        Dataset dataset = new Dataset(csv.toString(), 4);
        Files.deleteIfExists(csv);
        return new UnorderedPG(dataset, "label");
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("unordered-pg-test", ".csv");
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
