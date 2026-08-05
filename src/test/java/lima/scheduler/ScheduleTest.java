package lima.scheduler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;
import lima.lattice.PredicateRef;

/** Standalone smoke tests for Schedule's static helpers - run directly, no test framework wired up yet. */
public final class ScheduleTest {

    public static void main(String[] args) {
        testBestParentPgPicksLowestMean();
        testAdjacentNodesDedupsEndpoints();
        testNodesAboveThresholdFilters();
        testBuildTreeConnectsEveryNodeToRoot();
        testBuildTreeReusesSharedAncestorPaths();
        testInvertTreeRoundTripsWithBuildTree();
        System.out.println("All Schedule tests passed.");
    }

    private static void testBestParentPgPicksLowestMean() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Node b = lattice.getRoot().to(1, 0).to();

        state.updateNode(a, 1, 9997); // very low mean
        state.updateNode(b, 9997, 1); // very high mean

        List<Map.Entry<Integer, Lattice.Node>> candidates = new ArrayList<>();
        candidates.add(Map.entry(0, a));
        candidates.add(Map.entry(1, b));

        check(Schedule.bestParentPg(state, candidates) == 0, "should pick the candidate whose node has the lowest mean");
        System.out.println("testBestParentPgPicksLowestMean OK");
    }

    private static void testAdjacentNodesDedupsEndpoints() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        Lattice.Node root = lattice.getRoot();
        Lattice.Edge e1 = root.to(0, 0);
        Lattice.Edge e2 = root.to(0, 0).to().to(1, 0); // shares root.to(0,0)'s destination as its origin

        Set<Lattice.Edge> edges = new HashSet<>();
        edges.add(e1);
        edges.add(e2);

        Set<Lattice.Node> nodes = Schedule.adjacentNodes(edges);
        check(nodes.size() == 3, "root, A, and AB should be the three distinct endpoints, got " + nodes.size());
        check(nodes.contains(root), "adjacentNodes should include the shared root endpoint");
        System.out.println("testAdjacentNodesDedupsEndpoints OK");
    }

    private static void testNodesAboveThresholdFilters() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node lowEvidence = lattice.getRoot().to(0, 0).to(); // stays at Beta(1,1), high nodeGrad
        Lattice.Node highEvidence = lattice.getRoot().to(1, 0).to();
        state.updateNode(highEvidence, 4999, 4999); // lots of evidence -> low nodeGrad

        Set<Lattice.Node> nodes = Set.of(lowEvidence, highEvidence);
        Set<Lattice.Node> above = Schedule.nodesAboveThreshold(nodes, state, 0.01);

        check(above.contains(lowEvidence), "the barely-explored node should have a high nodeGrad, above threshold");
        check(!above.contains(highEvidence), "the heavily-sampled node should have a low nodeGrad, below threshold");
        System.out.println("testNodesAboveThresholdFilters OK");
    }

    private static void testBuildTreeConnectsEveryNodeToRoot() {
        Lattice lattice = new Lattice(new int[] {2, 2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node ab = lattice.getRoot().to(0, 0).to().to(1, 0).to();
        Lattice.Node ac = lattice.getRoot().to(0, 0).to().to(2, 0).to();

        Map<Lattice.Node, Integer> tree = Schedule.buildTree(Set.of(ab, ac), state);

        check(walksToRoot(ab, tree, lattice), "AB should have a complete path back to the root");
        check(walksToRoot(ac, tree, lattice), "AC should have a complete path back to the root");
        System.out.println("testBuildTreeConnectsEveryNodeToRoot OK");
    }

    private static void testBuildTreeReusesSharedAncestorPaths() {
        Lattice lattice = new Lattice(new int[] {2, 2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Node ab = a.to(1, 0).to();
        Lattice.Node ac = a.to(2, 0).to();

        // both AB and AC share A as their only viable intermediate ancestor
        // (their sole 1-predicate ancestor besides A would be B or C, but
        // the point here is just that recursion into A shouldn't blow up or
        // duplicate work when reached from two different requested nodes)
        Map<Lattice.Node, Integer> tree = Schedule.buildTree(Set.of(ab, ac), state);

        check(tree.containsKey(a), "the shared ancestor A should be present in the tree");
        check(walksToRoot(ab, tree, lattice), "AB should still resolve to the root");
        check(walksToRoot(ac, tree, lattice), "AC should still resolve to the root");
        System.out.println("testBuildTreeReusesSharedAncestorPaths OK");
    }

    private static void testInvertTreeRoundTripsWithBuildTree() {
        Lattice lattice = new Lattice(new int[] {2, 2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node ab = lattice.getRoot().to(0, 0).to().to(1, 0).to();
        Lattice.Node bc = lattice.getRoot().to(1, 0).to().to(2, 0).to();

        Map<Lattice.Node, Integer> tree = Schedule.buildTree(Set.of(ab, bc), state);
        Map<Lattice.Node, Set<PredicateRef>> children = Schedule.invertTree(tree, state);

        for (Map.Entry<Lattice.Node, Integer> entry : tree.entrySet()) {
            Lattice.Node node = entry.getKey();
            int pg = entry.getValue();
            if (pg == -1) {
                continue; // a root - has no parent edge to check
            }
            Lattice.Node parent = node.fr(pg).fr();
            Set<PredicateRef> parentChildren = children.get(parent);
            check(parentChildren != null, "parent " + parent + " should have a children entry");

            boolean reconstructs = false;
            for (PredicateRef ref : parentChildren) {
                if (parent.to(ref.pg(), ref.p()).to().equals(node)) {
                    reconstructs = true;
                }
            }
            check(reconstructs, "invertTree's children set for " + parent + " should contain the edge reaching " + node);
        }
        System.out.println("testInvertTreeRoundTripsWithBuildTree OK");
    }

    /** Follows tree[node] -> parent pg -> parent, ... until reaching a node
     * marked -1 (a root), returning false if the tree is broken (missing
     * entry) or the walk doesn't terminate within the lattice's max depth. */
    private static boolean walksToRoot(Lattice.Node node, Map<Lattice.Node, Integer> tree, Lattice lattice) {
        Lattice.Node current = node;
        for (int steps = 0; steps <= lattice.npgs(); steps++) {
            Integer pg = tree.get(current);
            if (pg == null) {
                return false;
            }
            if (pg == -1) {
                return true;
            }
            current = current.fr(pg).fr();
        }
        return false;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
