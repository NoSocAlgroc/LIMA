package lima.data.domain;

import lima.util.random.RandZ2;

/** Domain of tuple pairs drawn from a fixed range (e.g. a dataset's row count). */
public final class TPDomain implements Domain<TPDomainSet> {
    private final int range;
    private final RandZ2 rz2;

    public TPDomain(int range) {
        this(range, new RandZ2());
    }

    public TPDomain(int range, RandZ2 rz2) {
        this.range = range;
        this.rz2 = rz2;
    }

    @Override
    public TPDomainSet makeDomainSet(int size) {
        int[][] pairs = rz2.uniformGrid(size, range);
        return new TPDomainSet(pairs);
    }
}
