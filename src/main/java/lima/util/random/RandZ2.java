package lima.util.random;

import java.util.Arrays;
import java.util.Random;

/** Generates random pairs of distinct integers. */
public final class RandZ2 {
    private final int k; // target number of pairs per grid cell, used by uniformGrid
    private final Random random;

    public RandZ2() {
        this(64);
    }

    public RandZ2(int k) {
        this(k, new Random());
    }

    public RandZ2(int k, Random random) {
        this.k = k;
        this.random = random;
    }

    /**
     * Returns an (n, 2) array where each row is two different random
     * integers in [0, range).
     */
    public int[][] uniform(int n, int range) {
        return uniform(n, 0, range);
    }

    /**
     * Returns an (n, 2) array where each row is two different random
     * integers in [offset, offset + width).
     */
    public int[][] uniform(int n, int offset, int width) {
        int[][] pairs = new int[n][2];
        for (int i = 0; i < n; i++) {
            int a = offset + random.nextInt(width);
            int b = offset + random.nextInt(width);
            if (a == b) {
                b = offset + (b - offset + 1) % width;
            }
            pairs[i][0] = a;
            pairs[i][1] = b;
        }
        return pairs;
    }

    /**
     * Returns an (n, 2) array where column 0 is uniform in
     * [offset1, offset1+width1) and column 1 is uniform in
     * [offset2, offset2+width2), drawn independently - no distinctness is
     * enforced, since callers use this for two ranges that can never
     * overlap in the first place (so a == b is already impossible).
     */
    public int[][] uniformIndependent(int n, int offset1, int width1, int offset2, int width2) {
        int[][] pairs = new int[n][2];
        for (int i = 0; i < n; i++) {
            pairs[i][0] = offset1 + random.nextInt(width1);
            pairs[i][1] = offset2 + random.nextInt(width2);
        }
        return pairs;
    }

    /**
     * Same as `uniform`, but reordered for cache-friendly access.
     *
     * The [0, range) x [0, range) domain is subdivided into a grid with
     * roughly `k` pairs per cell (so ~n/k cells total, arranged in a
     * sqrt(n/k) x sqrt(n/k) layout). Pairs are then sorted by cell (x
     * first, then y), and by their original values within a cell, so pairs
     * that land near each other in space end up near each other in the
     * output too.
     */
    public int[][] uniformGrid(int n, int range) {
        int[][] pairs = uniform(n, range);

        int numCells = Math.max(1, n / k);
        int divisions = Math.max(1, (int) Math.round(Math.sqrt(numCells)));

        long[] cellX = new long[n];
        long[] cellY = new long[n];
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) {
            cellX[i] = ((long) pairs[i][0] * divisions) / range;
            cellY[i] = ((long) pairs[i][1] * divisions) / range;
            order[i] = i;
        }

        // most significant key last, matching np.lexsort((y, x, cellY, cellX))
        Arrays.sort(order, (i, j) -> {
            int cmp = Long.compare(cellX[i], cellX[j]);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Long.compare(cellY[i], cellY[j]);
            if (cmp != 0) {
                return cmp;
            }
            cmp = Integer.compare(pairs[i][0], pairs[j][0]);
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(pairs[i][1], pairs[j][1]);
        });

        int[][] result = new int[n][2];
        for (int i = 0; i < n; i++) {
            result[i] = pairs[order[i]];
        }
        return result;
    }
}
