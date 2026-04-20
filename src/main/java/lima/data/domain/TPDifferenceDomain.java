package lima.data.domain;

import lima.util.random.RandZ2;

/**
 * Domain of tuple pairs from [0,newRange)^2 excluding [0,oldRange)^2, i.e.
 * pairs involving at least one newly added row. Used to top up evidence
 * gathered on a smaller domain without resampling the old region.
 *
 * The difference region is split into three disjoint rectangles:
 *   A: old i x new j
 *   B: new i x old j
 *   C: new i x new j, excluding i == j
 * Each region is sampled in proportion to its number of valid pairs.
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
