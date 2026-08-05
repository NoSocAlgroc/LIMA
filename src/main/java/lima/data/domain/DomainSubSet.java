package lima.data.domain;

import java.util.Arrays;

/**
 * Adaptive sparse/dense subset of positions in a DomainSet of size `n`.
 *
 * Only needs `n` (the domain set's length), not the domain set itself.
 * Dense: bits packed into `packed` (MSB-first per byte). AND is a byte-level
 * bitop. Sparse: sorted position indices in `indices`. AND is a sorted-array
 * intersection. Exactly one of packed / indices is set; the other is null.
 */
public final class DomainSubSet {
    private static final double SPARSE_THRESHOLD = 0.1; // use index list when < 10% of positions match

    private final int n;
    private final byte[] packed;  // set iff dense
    private final int[] indices;  // set iff sparse
    private final int length;

    private DomainSubSet(int n, byte[] packed, int[] indices, int length) {
        this.n = n;
        this.packed = packed;
        this.indices = indices;
        this.length = length;
    }

    // ------------------------------------------------------------------
    // Construction helpers
    // ------------------------------------------------------------------

    public static DomainSubSet full(int n) {
        boolean[] mask = new boolean[n];
        Arrays.fill(mask, true);
        return new DomainSubSet(n, packBits(mask), null, n);
    }

    /** Choose representation from a sorted, duplicate-free index array. */
    public static DomainSubSet make(int n, int[] indices) {
        int k = indices.length;
        if (k < n * SPARSE_THRESHOLD) {
            return new DomainSubSet(n, null, indices.clone(), k);
        }
        boolean[] mask = new boolean[n];
        for (int idx : indices) {
            mask[idx] = true;
        }
        return new DomainSubSet(n, packBits(mask), null, k);
    }

    // ------------------------------------------------------------------
    // Public interface
    // ------------------------------------------------------------------

    /** Sorted array of matching positions into the domain set. */
    public int[] indices() {
        if (indices != null) {
            return indices;
        }
        boolean[] mask = unpackBits(packed, n);
        return toIndices(mask, length);
    }

    public int size() {
        return length;
    }

    public DomainSubSet and(DomainSubSet other) {
        if (this.n != other.n) {
            throw new IllegalArgumentException(
                "cannot intersect DomainSubSets over different-sized domains (" + this.n + " vs " + other.n + ")");
        }
        int n = this.n;

        // Dense & Dense - byte-level AND
        if (this.packed != null && other.packed != null) {
            byte[] result = new byte[packed.length];
            int count = 0;
            for (int i = 0; i < packed.length; i++) {
                result[i] = (byte) (packed[i] & other.packed[i]);
                count += Integer.bitCount(result[i] & 0xFF);
            }
            if (count < n * SPARSE_THRESHOLD) {
                int[] idx = toIndices(unpackBits(result, n), count);
                return new DomainSubSet(n, null, idx, count);
            }
            return new DomainSubSet(n, result, null, count);
        }

        // Sparse & Sparse - sorted-array intersection
        if (this.indices != null && other.indices != null) {
            return make(n, sortedIntersect(this.indices, other.indices));
        }

        // Mixed - check each sparse index against the packed bits of the dense side
        int[] sparseIdx = this.indices != null ? this.indices : other.indices;
        byte[] packedSide = this.indices != null ? other.packed : this.packed;
        return make(n, filterBySetBit(sparseIdx, packedSide));
    }

    // ------------------------------------------------------------------
    // Bit-packing helpers (MSB-first per byte, matching np.packbits)
    // ------------------------------------------------------------------

    private static byte[] packBits(boolean[] mask) {
        byte[] packed = new byte[(mask.length + 7) / 8];
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                packed[i >> 3] |= (byte) (1 << (7 - (i & 7)));
            }
        }
        return packed;
    }

    private static boolean[] unpackBits(byte[] packed, int n) {
        boolean[] mask = new boolean[n];
        for (int i = 0; i < n; i++) {
            int byteVal = packed[i >> 3] & 0xFF;
            mask[i] = ((byteVal >> (7 - (i & 7))) & 1) != 0;
        }
        return mask;
    }

    private static int[] toIndices(boolean[] mask, int count) {
        int[] result = new int[count];
        int j = 0;
        for (int i = 0; i < mask.length; i++) {
            if (mask[i]) {
                result[j++] = i;
            }
        }
        return result;
    }

    private static int[] sortedIntersect(int[] a, int[] b) {
        int[] buffer = new int[Math.min(a.length, b.length)];
        int i = 0, j = 0, count = 0;
        while (i < a.length && j < b.length) {
            if (a[i] == b[j]) {
                buffer[count++] = a[i];
                i++;
                j++;
            } else if (a[i] < b[j]) {
                i++;
            } else {
                j++;
            }
        }
        return Arrays.copyOf(buffer, count);
    }

    private static int[] filterBySetBit(int[] sparseIdx, byte[] packed) {
        int[] buffer = new int[sparseIdx.length];
        int count = 0;
        for (int idx : sparseIdx) {
            int byteVal = packed[idx >> 3] & 0xFF;
            if (((byteVal >> (7 - (idx & 7))) & 1) != 0) {
                buffer[count++] = idx;
            }
        }
        return Arrays.copyOf(buffer, count);
    }
}
