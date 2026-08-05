package lima.lattice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.util.stats.BetaDist;

/**
 * Concrete LatticeState: stores a BetaDist per explored node and a cache of
 * sound/candidate edges, maintained via a statistical soundness test as new
 * sampling results come in.
 */
public final class LIMALatticeState implements LatticeState {
    private final Lattice lattice;
    private final Map<Lattice.Node, BetaDist> dists = new HashMap<>();
    private final Set<Lattice.Edge> soundEdges = new HashSet<>();
    private final Set<Lattice.Edge> candidateEdges = new HashSet<>();

    public LIMALatticeState(Lattice lattice) {
        this.lattice = lattice;
        // Root always starts explored with uninformative prior
        dists.put(lattice.getRoot(), BetaDist.empty());

        // Seed: every root -> depth-1 edge is sound, and every
        // depth-1 -> depth-2 edge is a candidate.
        Lattice.Node root = lattice.getRoot();
        for (int pg = 0; pg < lattice.npgs(); pg++) {
            for (int p = 0; p < lattice.pgs(pg); p++) {
                Lattice.Edge edge = root.to(pg, p);
                soundEdges.add(edge);

                Lattice.Node child = edge.to();
                for (int pg2 = 0; pg2 < lattice.npgs(); pg2++) {
                    if (pg2 == pg) {
                        continue;
                    }
                    for (int p2 = 0; p2 < lattice.pgs(pg2); p2++) {
                        candidateEdges.add(child.to(pg2, p2));
                    }
                }
            }
        }
    }

    @Override
    public Lattice lattice() {
        return lattice;
    }

    @Override
    public Set<Lattice.Edge> soundEdges() {
        return soundEdges;
    }

    @Override
    public Set<Lattice.Edge> candidateEdges() {
        return candidateEdges;
    }

    @Override
    public BetaDist getDist(Lattice.Node n) {
        BetaDist dist = dists.get(n);
        return dist != null ? dist : BetaDist.empty();
    }

    public boolean isExplored(Lattice.Node n) {
        return dists.containsKey(n);
    }

    @Override
    public Iterator<Lattice.Node> explored() {
        return dists.keySet().iterator();
    }

    /** Add a new batch of (successes, failures) to a node's distribution. */
    public void updateNode(Lattice.Node n, long a, long b) {
        dists.computeIfAbsent(n, k -> BetaDist.empty()).add(a, b);
    }

    @Override
    public void update(Map<Lattice.Node, long[]> results, double threshold) {
        for (Map.Entry<Lattice.Node, long[]> entry : results.entrySet()) {
            long[] ab = entry.getValue();
            updateNode(entry.getKey(), ab[0], ab[1]);
        }
        updateSoundEdges(threshold);
    }

    private void updateSoundEdges(double threshold) {
        List<Lattice.Edge> newlySound = new ArrayList<>();
        for (Lattice.Edge edge : new ArrayList<>(candidateEdges)) {
            if (isSound(edge, threshold)) {
                candidateEdges.remove(edge);
                soundEdges.add(edge);
                newlySound.add(edge);
            }
        }
        for (Lattice.Edge edge : newlySound) {
            propagateSound(edge);
        }
    }

    /** Spanning-tree parent of n: remove the predicate whose removal gives
     * the most selective (lowest meanLO) parent node. */
    public Lattice.Node getTreeParent(Lattice.Node n) {
        int bestPg = bestParentPg(n);
        return n.fr(bestPg).fr();
    }

    /** Upward edge from tree-parent to n (parent -> n, adding bestPg). */
    public Lattice.Edge getTreeParentEdge(Lattice.Node n) {
        int bestPg = bestParentPg(n);
        Lattice.Node parent = n.fr(bestPg).fr();
        return parent.to(bestPg, n.get(bestPg));
    }

    private int bestParentPg(Lattice.Node n) {
        if (n.preds().isEmpty()) {
            throw new IllegalStateException("node has no active predicates, so it has no tree parent");
        }
        int bestPg = -1;
        double bestMeanLO = Double.POSITIVE_INFINITY;
        for (int pg : n.preds().keySet()) {
            double meanLO = getDist(n.fr(pg).fr()).meanLO;
            if (meanLO < bestMeanLO) {
                bestMeanLO = meanLO;
                bestPg = pg;
            }
        }
        return bestPg;
    }

    /**
     * Pre-populate nodes and sound edges for all lattice nodes up to depth k.
     *
     * Every node at depth &lt;= k gets an uninformative BetaDist(1,1), and
     * every upward edge between adjacent layers is added to soundEdges.
     * This lets the scheduler immediately propose depth-(k+1) candidates on
     * the first step, treating the shallower layers as already validated.
     */
    public void initializeToDepth(int k) {
        Set<Lattice.Node> visited = new HashSet<>();
        visited.add(lattice.getRoot());
        List<Lattice.Node> frontier = new ArrayList<>();
        frontier.add(lattice.getRoot());

        for (int depth = 0; depth < k; depth++) {
            List<Lattice.Node> nextFrontier = new ArrayList<>();
            for (Lattice.Node node : frontier) {
                for (int pg = 0; pg < lattice.npgs(); pg++) {
                    if (node.contains(pg)) {
                        continue;
                    }
                    for (int p = 0; p < lattice.pgs(pg); p++) {
                        Lattice.Edge edge = node.to(pg, p);
                        Lattice.Node child = edge.to();
                        soundEdges.add(edge);
                        if (!visited.contains(child)) {
                            visited.add(child);
                            dists.put(child, BetaDist.empty());
                            nextFrontier.add(child);
                        }
                    }
                }
            }
            frontier = nextFrontier;
        }
    }

    /**
     * Edge parent->child is sound if, for every predicate pg in parent,
     * nodeDist4(parent-pg, child-pg, parent, child) &lt; -threshold.
     *
     * Each comparison asks: does this edge add more selectivity than the
     * reference edge that lacks predicate pg?
     */
    private boolean isSound(Lattice.Edge edge, double threshold) {
        Lattice.Node parent = edge.fr();
        BetaDist from1Dist = getDist(parent);
        BetaDist to1Dist = getDist(edge.to());

        for (int pg : parent.preds().keySet()) {
            double z = supportZScore(edge, parent, from1Dist, to1Dist, pg);
            if (z >= -threshold) {
                return false;
            }
        }
        return true;
    }

    @Override
    public double minSoundZ(Lattice.Edge edge) {
        Lattice.Node parent = edge.fr();
        BetaDist from1Dist = getDist(parent);
        BetaDist to1Dist = getDist(edge.to());

        double best = Double.POSITIVE_INFINITY;
        for (int pg : parent.preds().keySet()) {
            double z = supportZScore(edge, parent, from1Dist, to1Dist, pg);
            best = Math.min(best, z);
        }
        return best;
    }

    /** The nodeDist4 z-score comparing `edge` against the reference edge
     * that lacks predicate `pg` from its parent. Shared by isSound (which
     * short-circuits on the first violation) and minSoundZ (which needs
     * every score to find the minimum). */
    private double supportZScore(
        Lattice.Edge edge, Lattice.Node parent, BetaDist from1Dist, BetaDist to1Dist, int pg
    ) {
        Lattice.Node subset = parent.fr(pg).fr();
        Lattice.Edge edge2 = subset.to(edge.pg, edge.p);
        BetaDist from2Dist = getDist(subset);
        BetaDist to2Dist = getDist(edge2.to());
        return BetaDist.nodeDist4(from1Dist, to1Dist, from2Dist, to2Dist);
    }

    /** Edge parent->child is a candidate if every parallel support edge is sound.
     *
     * For each predicate in parent, remove it and check that the
     * same-predicate edge from that reduced parent is in soundEdges. */
    private boolean isCandidate(Lattice.Edge edge) {
        Lattice.Node parent = edge.fr();
        for (int pg : parent.preds().keySet()) {
            Lattice.Edge supportEdge = parent.fr(pg).fr().to(edge.pg, edge.p);
            if (!soundEdges.contains(supportEdge)) {
                return false;
            }
        }
        return true;
    }

    /** Given newly-sound edge A->X, find supersets of A (excluding X) and
     * register any new candidate edges that add the same predicate from
     * those supersets. */
    private void propagateSound(Lattice.Edge edge) {
        Lattice.Node parent = edge.fr();
        for (int pg2 = 0; pg2 < lattice.npgs(); pg2++) {
            if (parent.contains(pg2) || pg2 == edge.pg) {
                continue;
            }
            for (int p2 = 0; p2 < lattice.pgs(pg2); p2++) {
                Lattice.Node superset = parent.to(pg2, p2).to();
                Lattice.Edge candidateEdge = superset.to(edge.pg, edge.p);
                if (isCandidate(candidateEdge)) {
                    candidateEdges.add(candidateEdge);
                }
            }
        }
    }
}
