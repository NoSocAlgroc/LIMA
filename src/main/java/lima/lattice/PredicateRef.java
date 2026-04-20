package lima.lattice;

/** A (predicate group, predicate index) pair - the label on a Lattice.Edge,
 * without the edge's endpoints attached. Used by Schedule/Sample to record
 * which edges lead to a node's children in a schedule tree. */
public record PredicateRef(int pg, int p) {
}
