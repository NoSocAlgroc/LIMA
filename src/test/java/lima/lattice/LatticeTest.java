package lima.lattice;

import java.util.HashSet;
import java.util.Set;

/** Standalone smoke tests for Lattice/Node/Edge - run directly, no test framework wired up yet. */
public final class LatticeTest {

    public static void main(String[] args) {
        testRootIsEmpty();
        testPgsAndMaxLatticeSize();
        testToAddsPredicate();
        testFrRemovesPredicate();
        testNodeEqualityIsOrderIndependentAndHashConsistent();
        testAddingAlreadyActiveGroupThrows();
        testRemovingAbsentGroupThrows();
        System.out.println("All Lattice tests passed.");
    }

    private static void testRootIsEmpty() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        Lattice.Node root = lattice.getRoot();
        check(root.size() == 0, "root should have no active predicates");
        check(root.preds().isEmpty(), "root's preds map should be empty");
        System.out.println("testRootIsEmpty OK");
    }

    private static void testPgsAndMaxLatticeSize() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        check(lattice.npgs() == 2, "npgs should equal the number of groups");
        check(lattice.pgs(0) == 2 && lattice.pgs(1) == 3, "pgs(i) should return group i's predicate count");
        check(lattice.maxLatticeSize() == 3 * 4, "maxLatticeSize should be product of (pgs[i]+1)");
        System.out.println("testPgsAndMaxLatticeSize OK");
    }

    private static void testToAddsPredicate() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        Lattice.Node root = lattice.getRoot();

        Lattice.Edge edge = root.to(0, 1);
        Lattice.Node child = edge.to();

        check(child.size() == 1, "child should have exactly one active predicate");
        check(child.contains(0), "child should have group 0 active");
        check(child.get(0) == 1, "child's predicate for group 0 should be 1");
        check(edge.fr().equals(root), "edge.fr() should be the original root");
        check(edge.pg == 0 && edge.p == 1, "edge should record (pg, p)");
        // root itself must stay unmodified (to() copies, never mutates in place)
        check(root.size() == 0, "root should be unaffected by building an edge off of it");
        System.out.println("testToAddsPredicate OK");
    }

    private static void testFrRemovesPredicate() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        Lattice.Node ab = lattice.getRoot().to(0, 1).to().to(1, 2).to(); // pg0=1, pg1=2

        Lattice.Edge edge = ab.fr(0);
        check(edge.to().equals(ab), "fr(pg)'s edge should point back to the original node");
        check(edge.pg == 0 && edge.p == 1, "fr(pg) should recover the predicate that was removed");

        Lattice.Node withoutPg0 = edge.fr();
        check(withoutPg0.size() == 1 && withoutPg0.contains(1) && withoutPg0.get(1) == 2,
            "removing pg0 should leave just pg1=2 active");
        System.out.println("testFrRemovesPredicate OK");
    }

    private static void testNodeEqualityIsOrderIndependentAndHashConsistent() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        Lattice.Node viaPg0First = lattice.getRoot().to(0, 1).to().to(1, 2).to();
        Lattice.Node viaPg1First = lattice.getRoot().to(1, 2).to().to(0, 1).to();

        check(viaPg0First.equals(viaPg1First), "nodes with the same final predicate set should be equal regardless of build order");
        check(viaPg0First.hashCode() == viaPg1First.hashCode(), "equal nodes must have equal hashCodes");

        Set<Lattice.Node> set = new HashSet<>();
        set.add(viaPg0First);
        set.add(viaPg1First);
        check(set.size() == 1, "a HashSet should treat them as the same node");
        System.out.println("testNodeEqualityIsOrderIndependentAndHashConsistent OK");
    }

    private static void testAddingAlreadyActiveGroupThrows() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        Lattice.Node a = lattice.getRoot().to(0, 1).to();
        boolean threw = false;
        try {
            a.to(0, 0);
        } catch (IllegalStateException e) {
            threw = true;
        }
        check(threw, "adding a predicate for an already-active group should throw");
        System.out.println("testAddingAlreadyActiveGroupThrows OK");
    }

    private static void testRemovingAbsentGroupThrows() {
        Lattice lattice = new Lattice(new int[] {2, 3});
        boolean threw = false;
        try {
            lattice.getRoot().fr(0);
        } catch (IllegalStateException e) {
            threw = true;
        }
        check(threw, "removing a predicate for a group that's not active should throw");
        System.out.println("testRemovingAbsentGroupThrows OK");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
