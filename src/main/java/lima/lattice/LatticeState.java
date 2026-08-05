package lima.lattice;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lima.util.stats.BetaDist;

/**
 * Abstract mutable belief state over the lattice.
 *
 * Exposes exactly what outside classes (Scheduler/Sampler/LIMA
 * implementations) actually use: lattice, soundEdges, candidateEdges, and
 * the methods below. Everything else (e.g. how sound/candidate edges get
 * maintained) is an implementation detail of a concrete implementation.
 */
public interface LatticeState {
    Lattice lattice();

    /** Live, not a copy. */
    Set<Lattice.Edge> soundEdges();

    /** Live, not a copy. */
    Set<Lattice.Edge> candidateEdges();

    BetaDist getDist(Lattice.Node n);

    /** Every node with a recorded distribution (i.e. sampled at least once). */
    Iterator<Lattice.Node> explored();

    /**
     * Add a sampling round's results (node -> {successes, failures}) to
     * each node's distribution, then refresh sound/candidate edges to
     * reflect the updated distributions.
     */
    void update(Map<Lattice.Node, long[]> results, double threshold);

    /**
     * The minimum (most negative) nodeDist4 z-score over every predicate pg
     * in the edge's parent - how sound `edge` is at its strongest
     * supporting comparison, on the same scale a soundness threshold
     * compares against.
     */
    double minSoundZ(Lattice.Edge edge);
}
