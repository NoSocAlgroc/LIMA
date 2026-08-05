package lima;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import lima.data.dataset.Dataset;

/**
 * CLI entry point: java -jar LIMA.jar &lt;dataset.csv&gt; &lt;approx&gt; &lt;rows&gt;
 *
 * &lt;rows&gt; is a comma-separated list of row counts n1[,n2,n3,...]. A single
 * value runs exactly as before: LIMA saturates on [0,n1) by distribution
 * variance (the usual nodeGrad/approx threshold), then stops. Extra values
 * each grow the active row range by that many rows (n1 -&gt; n1+n2 -&gt;
 * n1+n2+n3 -&gt; ...) and re-saturate - this time by sample count rather than
 * variance, topping up every already-explored node's evidence to stay
 * valid at the larger scale instead of resampling from scratch.
 */
public final class Main {
    private static final int MAX_ITERATIONS = 100; // per-stage safety bound, not expected to be hit in practice
    private static final String OUTPUT_PATH = "output.txt";

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: java -jar LIMA.jar <dataset.csv> <approx> <rows>");
            System.err.println("  <rows> is a comma-separated list, e.g. 1000 or 1000,500,500");
            System.exit(1);
            return;
        }

        String datasetPath = args[0];
        double approx = Double.parseDouble(args[1]);
        int[] rowCounts = parseRowCounts(args[2]);

        int totalRows = 0;
        for (int rows : rowCounts) {
            totalRows += rows;
        }
        Dataset dataset = new Dataset(datasetPath, totalRows);

        LIMA lima = new LIMA(dataset, approx, rowCounts[0]);
        int cumulativeRange = rowCounts[0];
        System.out.println("Stage 1: saturating on [0, " + cumulativeRange + ") by distribution variance...");
        runToConvergence(lima, "stage 1");

        for (int stage = 1; stage < rowCounts.length; stage++) {
            lima.expand(rowCounts[stage]);
            cumulativeRange += rowCounts[stage];
            System.out.println(
                "Stage " + (stage + 1) + ": expanding to [0, " + cumulativeRange + ") and topping up by sample count...");
            runToConvergence(lima, "stage " + (stage + 1));
        }

        writeDCs(lima.deadEndsText(), OUTPUT_PATH);
        System.out.println("Wrote " + OUTPUT_PATH);
    }

    private static int[] parseRowCounts(String arg) {
        String[] parts = arg.split(",");
        int[] rowCounts = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            rowCounts[i] = Integer.parseInt(parts[i].trim());
            if (rowCounts[i] <= 0) {
                throw new IllegalArgumentException("row counts must be positive, got " + rowCounts[i]);
            }
        }
        return rowCounts;
    }

    private static void runToConvergence(LIMA lima, String stageLabel) {
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            if (lima.step(i)) {
                System.out.println("  " + stageLabel + " converged after " + (i + 1) + " iteration(s)");
                return;
            }
        }
        System.err.println("  warning: " + stageLabel + " did not converge within " + MAX_ITERATIONS + " iterations");
    }

    private static void writeDCs(List<String> dcs, String path) throws IOException {
        try (PrintWriter writer = new PrintWriter(path, "UTF-8")) {
            for (String dc : dcs) {
                writer.println(dc);
            }
        }
    }
}
