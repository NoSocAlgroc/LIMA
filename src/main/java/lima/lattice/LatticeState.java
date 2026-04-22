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

    /** Conditional distribution of the edge's target given its source. */
    BetaDist getEdgeDist(Lattice.Edge edge);

    boolean isEdgeExplored(Lattice.Edge edge);

    /** Every edge with a recorded conditional distribution. */
    Iterable<Lattice.Edge> exploredEdges();

    /** Every node with a recorded distribution (i.e. sampled at least once). */
    Iterator<Lattice.Node> explored();

    /**
     * Add a sampling round's node results, then refresh sound/candidate
     * edges using node distributions (legacy path).
     */
    void update(Map<Lattice.Node, long[]> results, double threshold);

    /**
     * Add a sampling round's node and edge results, then refresh
     * sound/candidate edges using edge conditional distributions.
     */
    void update(Map<Lattice.Node, long[]> results, Map<Lattice.Edge, long[]> edgeResults, double threshold);

    /**
     * The minimum edge-interaction z-score over every predicate pg in the
     * edge's parent: how sound `edge` is at its strongest supporting
     * comparison.
     */
    double minSoundZ(Lattice.Edge edge);
}
