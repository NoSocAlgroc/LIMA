package lima;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Random;

import lima.data.dataset.Dataset;

/**
 * CLI entry point: java -jar LIMA.jar &lt;dataset.csv&gt; &lt;approx&gt; &lt;rows&gt; [seed]
 *
 * &lt;rows&gt; is a comma-separated list. The first value runs LIMA on
 * [0,n1); each later value expands the active range by that many rows and
 * re-runs the top-up phase. If no seed is given, a random one is chosen and
 * printed.
 */
public final class Main {
    private static final int MAX_ITERATIONS = 100; // per-stage safety bound, might be worth increasing max sample size instead of this if it is ever hit
    private static final String OUTPUT_PATH = "output.txt";

    public static void main(String[] args) throws IOException {


        //args= new String[]{"data/tax500k.csv", "0.0000001", "100000", "42"};
        if (args.length < 3 || args.length > 4) {
            System.err.println("Usage: java -jar LIMA.jar <dataset.csv> <approx> <rows> [seed]");
            System.err.println("  <rows> is a comma-separated list, e.g. 1000 or 1000,500,500");
            System.exit(1);
            return;
        }

        String datasetPath = args[0];
        double approx = Double.parseDouble(args[1]);
        int[] rowCounts = parseRowCounts(args[2]);
        long seed = args.length == 4 ? Long.parseLong(args[3]) : new Random().nextLong();
        //System.out.println("Seed: " + seed);

        int totalRows = 0;
        for (int rows : rowCounts) {
            totalRows += rows;
        }
        Dataset dataset = new Dataset(datasetPath, totalRows);

        LIMA lima = new LIMA(dataset, approx, rowCounts[0], seed);
        int cumulativeRange = rowCounts[0];
        //System.out.println("Stage 1: saturating on [0, " + cumulativeRange + ") by distribution variance...");
        runToConvergence(lima, "stage 1");

        for (int stage = 1; stage < rowCounts.length; stage++) {
            lima.expand(rowCounts[stage]);
            cumulativeRange += rowCounts[stage];
            //System.out.println(
            //    "Stage " + (stage + 1) + ": expanding to [0, " + cumulativeRange + ") and topping up by sample count...");
            runToConvergence(lima, "stage " + (stage + 1));
        }

        if (LIMA.ar) {
            writeMatrix(lima.relatedness(), OUTPUT_PATH);
        } else {
            writeDCs(lima.deadEndsText(), OUTPUT_PATH);
        }
        //System.out.println("Wrote " + OUTPUT_PATH);
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
                //System.out.println("  " + stageLabel + " converged after " + (i + 1) + " iteration(s)");
                return;
            }
        }
        //System.err.println("  warning: " + stageLabel + " did not converge within " + MAX_ITERATIONS + " iterations");
    }

    private static void writeDCs(List<String> dcs, String path) throws IOException {
        try (PrintWriter writer = new PrintWriter(path, "UTF-8")) {
            for (String dc : dcs) {
                writer.println(dc);
            }
        }
    }

    private static void writeMatrix(double[][] matrix, String path) throws IOException {
        try (PrintWriter writer = new PrintWriter(path, "UTF-8")) {
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    writer.println(i + " " + j + " " + matrix[i][j]);
                }
            }
        }
    }
}
