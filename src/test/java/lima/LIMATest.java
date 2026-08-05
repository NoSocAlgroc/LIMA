package lima;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import lima.data.dataset.Dataset;
import lima.lattice.Lattice;

/** Standalone smoke tests for LIMA - run directly, no test framework wired up yet. */
public final class LIMATest {

    private static final String DATASET_CSV = buildDatasetCsv();

    public static void main(String[] args) throws IOException {
        testGetStepSizeMatchesFormula();
        testFreshLIMAConsidersAllDepth1NodesDeadEnds();
        testNodeTextFormat();
        testHighApproxMakesStepReturnTrueImmediately();
        testStepsShrinkDeadEndsAndKeepRelatednessWellFormed();
        testExpandThrowsOnNonPositiveAdditionalRows();
        testExpandThenStepConvergesAndStaysWellFormed();
        System.out.println("All LIMA tests passed.");
    }

    private static void testGetStepSizeMatchesFormula() throws IOException {
        LIMA lima = buildLima(1e-6);
        check(lima.getStepSize(0) == 1 << 10, "iteration 0 should give 2^10");
        check(lima.getStepSize(7) == 1 << 17, "iteration 7 should give the capped 2^17");
        check(lima.getStepSize(50) == 1 << 17, "iterations beyond 7 should stay capped at 2^17");
        System.out.println("testGetStepSizeMatchesFormula OK");
    }

    private static void testFreshLIMAConsidersAllDepth1NodesDeadEnds() throws IOException {
        LIMA lima = buildLima(1e-6);
        // 3 columns * (4 or 2 preds each) = 4+4+2 = 10 depth-1 nodes, all
        // freshly-seeded sound with no evidence yet -> all dead ends
        Set<Lattice.Node> deadEnds = lima.deadEnds();
        check(deadEnds.size() == 10, "every freshly-seeded depth-1 node should start as a dead end, got " + deadEnds.size());

        List<String> texts = lima.deadEndsText();
        check(texts.size() == deadEnds.size(), "deadEndsText should have one entry per dead end");
        for (String text : texts) {
            check(text.startsWith("!(") && text.endsWith(")"), "dead-end text should be wrapped in \"!( ... )\", got: " + text);
        }
        System.out.println("testFreshLIMAConsidersAllDepth1NodesDeadEnds OK");
    }

    private static void testNodeTextFormat() throws IOException {
        LIMA lima = buildLima(1e-6);
        Lattice.Node node = rootChild(lima, 0, 2); // id(Integer), predicate 2 = "<"
        check(lima.nodeText(node).equals("!(id(Integer)<id(Integer))"),
            "unexpected node text: " + lima.nodeText(node));

        Lattice.Node labelNode = rootChild(lima, 2, 0); // label, predicate 0 = "="
        check(lima.nodeText(labelNode).equals("!(label=label)"),
            "unexpected node text: " + lima.nodeText(labelNode));
        System.out.println("testNodeTextFormat OK");
    }

    private static void testHighApproxMakesStepReturnTrueImmediately() throws IOException {
        LIMA lima = buildLima(1000.0); // unreachably high threshold
        check(lima.step(0), "an unreachably high approx threshold should give an empty schedule immediately");
        System.out.println("testHighApproxMakesStepReturnTrueImmediately OK");
    }

    private static void testStepsShrinkDeadEndsAndKeepRelatednessWellFormed() throws IOException {
        LIMA lima = buildLima(1e-6);
        int initialDeadEnds = lima.deadEnds().size();

        for (int i = 0; i < 5; i++) {
            if (lima.step(i)) {
                break;
            }
        }

        check(lima.deadEnds().size() <= initialDeadEnds,
            "sampling should only ever resolve dead ends (give them evidence), never create new ones from nothing");

        double[][] matrix = lima.relatedness();
        check(matrix.length == 3 && matrix[0].length == 3, "relatedness should be an npgs x npgs matrix (3x3 here)");
        for (int x = 0; x < 3; x++) {
            check(matrix[x][x] == 0.0, "the diagonal should always stay 0 - a group can't be related to itself");
            for (int c = 0; c < 3; c++) {
                check(matrix[x][c] >= 0.0, "relatedness entries should never be negative, got matrix[" + x + "][" + c + "]=" + matrix[x][c]);
            }
        }
        System.out.println("testStepsShrinkDeadEndsAndKeepRelatednessWellFormed OK");
    }

    private static void testExpandThrowsOnNonPositiveAdditionalRows() throws IOException {
        LIMA lima = buildLima(1e-6);
        boolean threwZero = false;
        try {
            lima.expand(0);
        } catch (IllegalArgumentException e) {
            threwZero = true;
        }
        check(threwZero, "expand(0) should be rejected");

        boolean threwNegative = false;
        try {
            lima.expand(-5);
        } catch (IllegalArgumentException e) {
            threwNegative = true;
        }
        check(threwNegative, "expand(-5) should be rejected");
        System.out.println("testExpandThrowsOnNonPositiveAdditionalRows OK");
    }

    private static void testExpandThenStepConvergesAndStaysWellFormed() throws IOException {
        Path csv = writeTempCsv(DATASET_CSV);
        Dataset dataset = new Dataset(csv.toString(), 20); // load all 20 rows up front
        Files.deleteIfExists(csv);
        LIMA lima = new LIMA(dataset, 1e-6, 10); // but only start on the first 10

        runToConvergence(lima, 200);
        lima.expand(10); // grow to the full 20 rows
        runToConvergence(lima, 500);

        double[][] matrix = lima.relatedness();
        check(matrix.length == 3 && matrix[0].length == 3, "relatedness should stay an npgs x npgs matrix after expand");
        for (int x = 0; x < 3; x++) {
            check(matrix[x][x] == 0.0, "the diagonal should still stay 0 after expand");
            for (int c = 0; c < 3; c++) {
                check(matrix[x][c] >= 0.0, "relatedness entries should stay non-negative after expand");
            }
        }
        check(lima.deadEndsText() != null, "deadEndsText should still work without crashing after expand");
        System.out.println("testExpandThenStepConvergesAndStaysWellFormed OK");
    }

    private static void runToConvergence(LIMA lima, int maxIterations) {
        for (int i = 0; i < maxIterations; i++) {
            if (lima.step(i)) {
                return;
            }
        }
        throw new AssertionError("FAILED: did not converge (schedule never emptied) within " + maxIterations + " iterations");
    }

    // ------------------------------------------------------------------

    private static Lattice.Node rootChild(LIMA lima, int pg, int p) throws IOException {
        // LIMA doesn't expose its lattice directly, so rebuild an
        // equivalent root node the same way LIMA's constructor would:
        // pgs = [4, 4, 2] for [id(Integer), score(Double), label]
        Lattice lattice = new Lattice(new int[] {4, 4, 2});
        return lattice.getRoot().to(pg, p).to();
    }

    private static LIMA buildLima(double approx) throws IOException {
        Path csv = writeTempCsv(DATASET_CSV);
        Dataset dataset = new Dataset(csv.toString(), 20);
        Files.deleteIfExists(csv);
        return new LIMA(dataset, approx);
    }

    private static String buildDatasetCsv() {
        StringBuilder sb = new StringBuilder("id(Integer),score(Double),label\n");
        for (int i = 0; i < 20; i++) {
            sb.append(i).append(',').append(i % 5).append(',').append(i % 3 == 0 ? "a" : "b").append('\n');
        }
        return sb.toString();
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("lima-test", ".csv");
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
