package lima.data.domain;

import lima.util.random.RandZ2;

/**
 * Domain of tuple pairs drawn uniformly from [0,newRange) x [0,newRange)
 * minus [0,oldRange) x [0,oldRange) - every pair that involves at least one
 * row at or beyond oldRange. Used to "top up" evidence gathered on a
 * smaller domain so it stays valid after the underlying dataset grows,
 * without re-sampling the already-covered old x old region from scratch.
 *
 * The difference region is the union of three disjoint rectangles
 * (splitting [0,newRange) into [0,oldRange) and [oldRange,newRange) on both
 * axes, and dropping the one quadrant that's entirely the excluded old
 * region):
 *   A: old i x new j - oldRange x newRowCount
 *   B: new i x old j - newRowCount x oldRange
 *   C: new i x new j - newRowCount x newRowCount, minus its newRowCount
 *      self-pairs (i == j) - the only one of the three that can ever
 *      produce those, since A has i &lt; oldRange &lt;= j and B has
 *      i &gt;= oldRange &gt; j, both making i != j automatic.
 * Each draw's region is picked weighted by that region's number of valid
 * pairs, then drawn uniformly within it.
 */
public final class TPDifferenceDomain implements Domain<TPDomainSet> {
    private final int oldRange;
    private final int newRange;
    private final RandZ2 rz2;

    public TPDifferenceDomain(int oldRange, int newRange) {
        this(oldRange, newRange, new RandZ2());
    }

    public TPDifferenceDomain(int oldRange, int newRange, RandZ2 rz2) {
        if (oldRange < 0 || newRange <= oldRange) {
            throw new IllegalArgumentException(
                "newRange (" + newRange + ") must be greater than oldRange (" + oldRange + ")");
        }
        this.oldRange = oldRange;
        this.newRange = newRange;
        this.rz2 = rz2;
    }

    @Override
    public TPDomainSet makeDomainSet(int size) {
        int newRowCount = newRange - oldRange;

        long weightA = (long) oldRange * newRowCount;
        long weightB = (long) newRowCount * oldRange;
        long weightC = (long) newRowCount * (newRowCount - 1L);
        long total = weightA + weightB + weightC;
        if (total <= 0) {
            throw new IllegalStateException(
                "difference region [" + oldRange + ", " + newRange + ") has no valid distinct pairs to sample");
        }

        int countA = (int) Math.round(size * weightA / (double) total);
        int countB = (int) Math.round(size * weightB / (double) total);
        int countC = size - countA - countB;

        int[][] pairs = new int[size][];
        int pos = 0;
        pos = copyInto(pairs, pos, sampleRegionA(countA, newRowCount));
        pos = copyInto(pairs, pos, sampleRegionB(countB, newRowCount));
        copyInto(pairs, pos, sampleRegionC(countC, newRowCount));

        return new TPDomainSet(pairs);
    }

    /** old i in [0, oldRange), new j in [oldRange, newRange). */
    private int[][] sampleRegionA(int count, int newRowCount) {
        return rz2.uniformIndependent(count, 0, oldRange, oldRange, newRowCount);
    }

    /** new i in [oldRange, newRange), old j in [0, oldRange). */
    private int[][] sampleRegionB(int count, int newRowCount) {
        return rz2.uniformIndependent(count, oldRange, newRowCount, 0, oldRange);
    }

    /** new i, new j, both in [oldRange, newRange), i != j. */
    private int[][] sampleRegionC(int count, int newRowCount) {
        return rz2.uniform(count, oldRange, newRowCount);
    }

    private static int copyInto(int[][] dest, int destPos, int[][] src) {
        System.arraycopy(src, 0, dest, destPos, src.length);
        return destPos + src.length;
    }
}
