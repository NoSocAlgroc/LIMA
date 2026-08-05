package lima.data.domain;

/** Abstract indexable set of values: an instantiated subspace of a Domain. */
public interface DomainSet<T> {

    /** Number of elements in this set. */
    int size();

    /** Return the value(s) at the given position(s), as a fresh copy. */
    T[] get(int[] indices);

    /**
     * Return every value in this set, as if indexed by the full range.
     *
     * Implementations should avoid the copy that {@code get} over every
     * index would make - e.g. by returning the backing array directly when
     * it already holds exactly this data.
     */
    T[] getFull();

    /** Return a DomainSubSet covering every position of this set. */
    default DomainSubSet full() {
        return DomainSubSet.full(size());
    }
}
