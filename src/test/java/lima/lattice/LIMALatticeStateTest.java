package lima.lattice;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import lima.util.stats.BetaDist;

/** Standalone smoke tests for LIMALatticeState - run directly, no test framework wired up yet. */
public final class LIMALatticeStateTest {

    public static void main(String[] args) {
        testConstructorSeedsRootAndSoundEdges();
        testConstructorSeedsCandidateEdges();
        testGetDistDefaultsToEmptyWithoutStoring();
        testUpdateNodeAccumulates();
        testIsExploredAndExploredIterator();
        testUpdatePromotesSoundEdgeAndPropagatesNewCandidates();
        testGetTreeParentPicksLowestMeanLOAncestor();
        testGetTreeParentOnRootThrows();
        testGetTreeParentEdgeMatchesGetTreeParent();
        testInitializeToDepth();
        System.out.println("All LIMALatticeState tests passed.");
    }

    private static void testConstructorSeedsRootAndSoundEdges() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        check(state.isExplored(lattice.getRoot()), "root should be explored from construction");
        check(state.getDist(lattice.getRoot()).a == 1, "root's prior should be uninformative Beta(1,1)");

        // every root -> depth-1 edge should be sound
        for (int pg = 0; pg < 2; pg++) {
            for (int p = 0; p < 2; p++) {
                Lattice.Edge edge = lattice.getRoot().to(pg, p);
                check(state.soundEdges().contains(edge), "root -> depth-1 edge (pg=" + pg + ",p=" + p + ") should be seeded sound");
            }
        }
        System.out.println("testConstructorSeedsRootAndSoundEdges OK");
    }

    private static void testConstructorSeedsCandidateEdges() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Edge candidate = a.to(1, 0);
        check(state.candidateEdges().contains(candidate), "depth-1 -> depth-2 edge should be seeded as a candidate");
        check(!state.soundEdges().contains(candidate), "a freshly-seeded candidate should not already be sound");
        System.out.println("testConstructorSeedsCandidateEdges OK");
    }

    private static void testGetDistDefaultsToEmptyWithoutStoring() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node unexplored = lattice.getRoot().to(0, 1).to();

        BetaDist dist = state.getDist(unexplored);
        check(dist.a == 1 && dist.b == 1, "an unexplored node's dist should default to Beta(1,1)");
        check(!state.isExplored(unexplored), "merely calling getDist should not mark the node as explored");
        System.out.println("testGetDistDefaultsToEmptyWithoutStoring OK");
    }

    private static void testUpdateNodeAccumulates() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node root = lattice.getRoot();

        state.updateNode(root, 5, 3);
        state.updateNode(root, 2, 1);

        BetaDist dist = state.getDist(root);
        check(dist.a == 1 + 5 + 2 && dist.b == 1 + 3 + 1, "repeated updateNode calls should accumulate onto the same node");
        System.out.println("testUpdateNodeAccumulates OK");
    }

    private static void testIsExploredAndExploredIterator() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node node = lattice.getRoot().to(0, 1).to();

        check(!state.isExplored(node), "node should start unexplored");
        state.updateNode(node, 1, 1);
        check(state.isExplored(node), "updateNode should mark the node explored");

        boolean foundRoot = false;
        Iterator<Lattice.Node> it = state.explored();
        while (it.hasNext()) {
            if (it.next().equals(lattice.getRoot())) {
                foundRoot = true;
            }
        }
        check(foundRoot, "explored() should include the root, seeded at construction");
        System.out.println("testIsExploredAndExploredIterator OK");
    }

    /**
     * Engineers a 3-predicate-group lattice ([2,2,2]) where root and every
     * single-predicate node (A, B, C) are non-selective (mean ~0.5, lots of
     * evidence), while every pair (AB, AC, BC) is extremely selective (mean
     * ~0.0002). This makes all six depth1->depth2 "pair-forming" edges
     * (A->AB, B->AB, A->AC, C->AC, B->BC, C->BC) sound in one update() call,
     * which should in turn propagate three brand-new depth2->depth3
     * candidates (AB->ABC, AC->ABC, BC->ABC) that weren't there before.
     */
    private static void testUpdatePromotesSoundEdgeAndPropagatesNewCandidates() {
        Lattice lattice = new Lattice(new int[] {2, 2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node root = lattice.getRoot();
        Lattice.Node a = root.to(0, 0).to();
        Lattice.Node b = root.to(1, 0).to();
        Lattice.Node c = root.to(2, 0).to();
        Lattice.Node ab = a.to(1, 0).to();
        Lattice.Node ac = a.to(2, 0).to();
        Lattice.Node bc = b.to(2, 0).to();

        Lattice.Edge aToAb = a.to(1, 0);
        Lattice.Edge bToAb = b.to(0, 0);
        Lattice.Edge aToAc = a.to(2, 0);
        Lattice.Edge cToAc = c.to(0, 0);
        Lattice.Edge bToBc = b.to(2, 0);
        Lattice.Edge cToBc = c.to(1, 0);

        Lattice.Edge abToAbc = ab.to(2, 0);
        Lattice.Edge acToAbc = ac.to(1, 0);
        Lattice.Edge bcToAbc = bc.to(0, 0);

        // sanity: none of the depth-3 edges are candidates yet
        check(!state.candidateEdges().contains(abToAbc), "sanity: AB->ABC shouldn't be a candidate before propagation");
        check(!state.candidateEdges().contains(acToAbc), "sanity: AC->ABC shouldn't be a candidate before propagation");
        check(!state.candidateEdges().contains(bcToAbc), "sanity: BC->ABC shouldn't be a candidate before propagation");

        Map<Lattice.Node, long[]> results = new HashMap<>();
        results.put(root, new long[] {4999, 4999});
        results.put(a, new long[] {4999, 4999});
        results.put(b, new long[] {4999, 4999});
        results.put(c, new long[] {4999, 4999});
        results.put(ab, new long[] {1, 9997});
        results.put(ac, new long[] {1, 9997});
        results.put(bc, new long[] {1, 9997});

        state.update(results, 3.0);

        check(state.soundEdges().contains(aToAb), "A->AB should have become sound");
        check(state.soundEdges().contains(bToAb), "B->AB should have become sound");
        check(state.soundEdges().contains(aToAc), "A->AC should have become sound");
        check(state.soundEdges().contains(cToAc), "C->AC should have become sound");
        check(state.soundEdges().contains(bToBc), "B->BC should have become sound");
        check(state.soundEdges().contains(cToBc), "C->BC should have become sound");

        check(state.candidateEdges().contains(abToAbc), "AB->ABC should have been propagated as a new candidate");
        check(state.candidateEdges().contains(acToAbc), "AC->ABC should have been propagated as a new candidate");
        check(state.candidateEdges().contains(bcToAbc), "BC->ABC should have been propagated as a new candidate");
        System.out.println("testUpdatePromotesSoundEdgeAndPropagatesNewCandidates OK");
    }

    private static void testGetTreeParentPicksLowestMeanLOAncestor() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Node b = lattice.getRoot().to(1, 0).to();
        Lattice.Node ab = a.to(1, 0).to();

        // B is far more selective (lower meanLO) than A
        state.updateNode(a, 4999, 4999);
        state.updateNode(b, 1, 9997);

        Lattice.Node parent = state.getTreeParent(ab);
        check(parent.equals(b), "the tree parent should be the ancestor with the lower (more selective) meanLO, i.e. B");
        System.out.println("testGetTreeParentPicksLowestMeanLOAncestor OK");
    }

    private static void testGetTreeParentOnRootThrows() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        boolean threw = false;
        try {
            state.getTreeParent(lattice.getRoot());
        } catch (IllegalStateException e) {
            threw = true;
        }
        check(threw, "the root has no active predicates, so it has no tree parent");
        System.out.println("testGetTreeParentOnRootThrows OK");
    }

    private static void testGetTreeParentEdgeMatchesGetTreeParent() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Node b = lattice.getRoot().to(1, 0).to();
        Lattice.Node ab = a.to(1, 0).to();

        state.updateNode(a, 4999, 4999);
        state.updateNode(b, 1, 9997);

        Lattice.Node parent = state.getTreeParent(ab);
        Lattice.Edge parentEdge = state.getTreeParentEdge(ab);

        check(parentEdge.fr().equals(parent), "getTreeParentEdge's from-node should match getTreeParent");
        check(parentEdge.to().equals(ab), "getTreeParentEdge should point at n itself");
        System.out.println("testGetTreeParentEdgeMatchesGetTreeParent OK");
    }

    private static void testInitializeToDepth() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);

        state.initializeToDepth(2);

        int exploredCount = 0;
        Iterator<Lattice.Node> it = state.explored();
        while (it.hasNext()) {
            it.next();
            exploredCount++;
        }
        // root(1) + depth1(2 groups * 2 preds = 4) + depth2(2*2 combinations = 4) = 9
        check(exploredCount == 9, "all nodes up to depth 2 should be explored, got " + exploredCount);

        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        Lattice.Node ab = a.to(1, 0).to();
        check(state.soundEdges().contains(lattice.getRoot().to(0, 0)), "root->A should be marked sound");
        check(state.soundEdges().contains(a.to(1, 0)), "A->AB should be marked sound");
        check(state.getDist(ab).a == 1 && state.getDist(ab).b == 1, "depth-2 nodes should get an uninformative prior");
        System.out.println("testInitializeToDepth OK");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
