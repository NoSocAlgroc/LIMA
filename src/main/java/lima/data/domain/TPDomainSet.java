package lima.data.domain;

/** A DomainSet of tuple pairs: an (n, 2) array of row-index pairs. */
public final class TPDomainSet implements DomainSet<int[]> {
    private final int[][] pairs;

    public TPDomainSet(int[][] pairs) {
        this.pairs = pairs;
    }

    @Override
    public int size() {
        return pairs.length;
    }

    @Override
    public int[][] get(int[] indices) {
        int[][] result = new int[indices.length][];
        for (int i = 0; i < indices.length; i++) {
            result[i] = pairs[indices[i]].clone(); // fancy indexing always copies
        }
        return result;
    }

    @Override
    public int[][] getFull() {
        return pairs; // already exactly this data, no copy needed
    }
}
