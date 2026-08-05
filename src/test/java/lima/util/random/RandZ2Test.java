package lima.util.random;

import java.util.Arrays;
import java.util.Random;

/** Standalone smoke tests for RandZ2 - run directly, no test framework wired up yet. */
public final class RandZ2Test {

    public static void main(String[] args) {
        testUniformDistinctAndInRange();
        testUniformGridSameMultisetReordered();
        testUniformGridSortedByCell();
        testUniformGridSingleCellStillDistinct();
        testDefaultConstructorProducesRandomPairs();
        testUniformWithOffsetStaysInRangeAndDistinct();
        testUniformWithZeroOffsetMatchesPlainUniform();
        testUniformIndependentStaysInDisjointRanges();
        System.out.println("All RandZ2 tests passed.");
    }

    private static void testUniformDistinctAndInRange() {
        RandZ2 rz2 = new RandZ2(64, new Random(1));
        int range = 37;
        int[][] pairs = rz2.uniform(2000, range);

        for (int[] pair : pairs) {
            check(pair[0] >= 0 && pair[0] < range, "a should be in [0, range)");
            check(pair[1] >= 0 && pair[1] < range, "b should be in [0, range)");
            check(pair[0] != pair[1], "a and b should never be equal");
        }
        System.out.println("testUniformDistinctAndInRange OK");
    }

    private static void testUniformGridSameMultisetReordered() {
        int n = 500;
        int range = 1000;

        int[][] plain = new RandZ2(64, new Random(42)).uniform(n, range);
        int[][] gridded = new RandZ2(64, new Random(42)).uniformGrid(n, range);

        check(plain.length == gridded.length, "uniformGrid should return the same number of pairs");
        check(sortedCopy(plain).equals(sortedCopy(gridded)),
            "uniformGrid should be a reordering of the same pairs uniform() would draw from an identically-seeded generator");

        System.out.println("testUniformGridSameMultisetReordered OK");
    }

    private static void testUniformGridSortedByCell() {
        int n = 800;
        int range = 500;
        int k = 64;
        int[][] pairs = new RandZ2(k, new Random(7)).uniformGrid(n, range);

        int numCells = Math.max(1, n / k);
        int divisions = Math.max(1, (int) Math.round(Math.sqrt(numCells)));

        long prevCellX = Long.MIN_VALUE, prevCellY = Long.MIN_VALUE;
        int prevX = Integer.MIN_VALUE, prevY = Integer.MIN_VALUE;
        boolean firstInCell = true;

        for (int[] pair : pairs) {
            long cellX = ((long) pair[0] * divisions) / range;
            long cellY = ((long) pair[1] * divisions) / range;

            check(cellX > prevCellX || (cellX == prevCellX && cellY >= prevCellY),
                "pairs should be non-decreasing by (cellX, cellY)");

            if (cellX == prevCellX && cellY == prevCellY && !firstInCell) {
                boolean sameCellOrdered = pair[0] > prevX || (pair[0] == prevX && pair[1] >= prevY);
                check(sameCellOrdered, "within a cell, pairs should be non-decreasing by (x, y)");
            }

            prevCellX = cellX;
            prevCellY = cellY;
            prevX = pair[0];
            prevY = pair[1];
            firstInCell = false;
        }
        System.out.println("testUniformGridSortedByCell OK");
    }

    private static void testUniformGridSingleCellStillDistinct() {
        // k larger than n forces numCells (and divisions) down to 1 - every
        // pair lands in the same single cell, degrading uniformGrid to a
        // plain sort by (x, y). Should still hold every uniform() invariant.
        RandZ2 rz2 = new RandZ2(10_000, new Random(3));
        int[][] pairs = rz2.uniformGrid(50, 20);
        check(pairs.length == 50, "should still return n pairs");
        for (int[] pair : pairs) {
            check(pair[0] != pair[1], "a and b should never be equal even in the degenerate single-cell case");
        }
        System.out.println("testUniformGridSingleCellStillDistinct OK");
    }

    private static void testDefaultConstructorProducesRandomPairs() {
        RandZ2 rz2 = new RandZ2();
        int range = 10;
        int[][] pairs = rz2.uniform(100, range);
        check(pairs.length == 100, "should return the requested number of pairs");
        for (int[] pair : pairs) {
            check(pair[0] >= 0 && pair[0] < range && pair[1] >= 0 && pair[1] < range, "values should be in range");
            check(pair[0] != pair[1], "a and b should never be equal");
        }
        System.out.println("testDefaultConstructorProducesRandomPairs OK");
    }

    private static void testUniformWithOffsetStaysInRangeAndDistinct() {
        RandZ2 rz2 = new RandZ2(64, new Random(9));
        int offset = 1000;
        int width = 25;
        int[][] pairs = rz2.uniform(500, offset, width);

        for (int[] pair : pairs) {
            check(pair[0] >= offset && pair[0] < offset + width, "a should be in [offset, offset+width)");
            check(pair[1] >= offset && pair[1] < offset + width, "b should be in [offset, offset+width)");
            check(pair[0] != pair[1], "a and b should never be equal");
        }
        System.out.println("testUniformWithOffsetStaysInRangeAndDistinct OK");
    }

    private static void testUniformWithZeroOffsetMatchesPlainUniform() {
        int[][] viaPlain = new RandZ2(64, new Random(55)).uniform(200, 40);
        int[][] viaOffset = new RandZ2(64, new Random(55)).uniform(200, 0, 40);
        check(java.util.Arrays.deepEquals(viaPlain, viaOffset),
            "uniform(n, range) should behave exactly like uniform(n, 0, range) given the same seed");
        System.out.println("testUniformWithZeroOffsetMatchesPlainUniform OK");
    }

    private static void testUniformIndependentStaysInDisjointRanges() {
        RandZ2 rz2 = new RandZ2(64, new Random(3));
        int[][] pairs = rz2.uniformIndependent(500, 0, 10, 10, 5); // [0,10) and [10,15) - disjoint

        for (int[] pair : pairs) {
            check(pair[0] >= 0 && pair[0] < 10, "column 0 should be in [0,10)");
            check(pair[1] >= 10 && pair[1] < 15, "column 1 should be in [10,15)");
            check(pair[0] != pair[1], "disjoint ranges should never produce equal values");
        }
        System.out.println("testUniformIndependentStaysInDisjointRanges OK");
    }

    private static java.util.List<String> sortedCopy(int[][] pairs) {
        java.util.List<String> encoded = new java.util.ArrayList<>();
        for (int[] pair : pairs) {
            encoded.add(pair[0] + "," + pair[1]);
        }
        java.util.Collections.sort(encoded);
        return encoded;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
