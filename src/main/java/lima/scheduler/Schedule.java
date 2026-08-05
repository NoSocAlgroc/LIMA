package lima.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.lattice.PredicateRef;
import lima.util.stats.BetaDist;

/**
 * A sample schedule: the sampling tree for this round.
 *
 * `parents` maps each node to be sampled to its parent's pg (-1 for a
 * root); `children` is its inverse - each node to the set of (pg, p)
 * edges leading to its children in the tree.
 */
public final class Schedule {
    public final Map<Lattice.Node, Integer> parents;
    public final Map<Lattice.Node, Set<PredicateRef>> children;

    public Schedule() {
        this(new HashMap<>(), new HashMap<>());
    }

    public Schedule(Map<Lattice.Node, Integer> parents, Map<Lattice.Node, Set<PredicateRef>> children) {
        this.parents = parents;
        this.children = children;
    }

    /** Given `candidates` (pg, node.fr(pg).fr()) pairs, the pg whose parent has the smallest mean. */
    public static int bestParentPg(LatticeState state, List<Map.Entry<Integer, Lattice.Node>> candidates) {
        Map.Entry<Integer, Lattice.Node> best = null;
        double bestMean = Double.POSITIVE_INFINITY;
        for (Map.Entry<Integer, Lattice.Node> candidate : candidates) {
            double mean = state.getDist(candidate.getValue()).mean;
            if (mean < bestMean) {
                bestMean = mean;
                best = candidate;
            }
        }
        return best.getKey();
    }

    /**
     * Make sure `node` has a path to the root recorded in `tree`.
     *
     * `roots` must include the true lattice root (or another node already
     * guaranteed to terminate the recursion) - `node.preds()` being empty
     * is not checked separately here.
     */
    private static void ensurePath(
        Lattice.Node node, LatticeState state, Set<Lattice.Node> roots, Map<Lattice.Node, Integer> tree
    ) {
        if (tree.containsKey(node)) {
            return;
        }
        if (roots.contains(node)) {
            tree.put(node, -1);
            return;
        }

        List<Map.Entry<Integer, Lattice.Node>> candidates = new ArrayList<>();
        for (int pg : node.preds().keySet()) {
            candidates.add(Map.entry(pg, node.fr(pg).fr()));
        }
        for (Map.Entry<Integer, Lattice.Node> candidate : candidates) {
            ensurePath(candidate.getValue(), state, roots, tree);
        }
        tree.put(node, bestParentPg(state, candidates));
    }

    /** Build a tree (node -> parent pg) containing a path from every node in `nodes` to the root. */
    public static Map<Lattice.Node, Integer> buildTree(Set<Lattice.Node> nodes, LatticeState state) {
        Map<Lattice.Node, Integer> tree = new HashMap<>();
        Set<Lattice.Node> roots = Set.of(state.lattice().getRoot());
        for (Lattice.Node node : nodes) {
            ensurePath(node, state, roots, tree);
        }
        return tree;
    }

    /** Invert a parent tree (node -> parent pg) into a children map: each
     * node to the set of (pg, p) edges leading to its children. */
    public static Map<Lattice.Node, Set<PredicateRef>> invertTree(Map<Lattice.Node, Integer> tree, LatticeState state) {
        Map<Lattice.Node, Set<PredicateRef>> children = new HashMap<>();
        for (Map.Entry<Lattice.Node, Integer> entry : tree.entrySet()) {
            Lattice.Node node = entry.getKey();
            int pg = entry.getValue();
            if (pg == -1) {
                continue;
            }
            Lattice.Edge edge = node.fr(pg); // the edge parent -> node
            Lattice.Node parent = edge.fr();
            children.computeIfAbsent(parent, k -> new HashSet<>()).add(new PredicateRef(edge.pg, edge.p));
        }
        return children;
    }

    /** All nodes touched by `edges`: both the fr() and to() endpoint of each. */
    public static Set<Lattice.Node> adjacentNodes(Set<Lattice.Edge> edges) {
        Set<Lattice.Node> nodes = new HashSet<>();
        for (Lattice.Edge e : edges) {
            nodes.add(e.fr());
            nodes.add(e.to());
        }
        return nodes;
    }

    /** Nodes whose distribution's nodeGrad exceeds `threshold`. */
    public static Set<Lattice.Node> nodesAboveThreshold(Set<Lattice.Node> nodes, LatticeState state, double threshold) {
        Set<Lattice.Node> result = new HashSet<>();
        for (Lattice.Node n : nodes) {
            if (BetaDist.nodeGrad(state.getDist(n)) > threshold) {
                result.add(n);
            }
        }
        return result;
    }
}
