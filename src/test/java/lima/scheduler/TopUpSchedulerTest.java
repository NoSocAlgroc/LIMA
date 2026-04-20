package lima.scheduler;

import java.util.Map;

import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;

/** Standalone smoke tests for TopUpScheduler - run directly, no test framework wired up yet. */
public final class TopUpSchedulerTest {

    public static void main(String[] args) {
        testComputeTargetsScalesProportionally();
        testComputeTargetsOnlyIncludesExploredNodes();
        testScheduleIncludesNodesBelowTargetAndRoutesToRoot();
        testScheduleExcludesIsolatedNodeAtOrAboveTarget();
        testThresholdParameterIsIgnored();
        System.out.println("All TopUpScheduler tests passed.");
    }

    private static void testComputeTargetsScalesProportionally() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        state.updateNode(a, 3, 7); // a: 1+3=4, b: 1+7=8, x=12

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, 10, 20); // scale = 400/100 = 4

        check(targets.get(lattice.getRoot()) == 8L, "root's x=2 (seeded prior) should scale to target 8, got " + targets.get(lattice.getRoot()));
        check(targets.get(a) == 48L, "A's x=12 should scale to target 48, got " + targets.get(a));
        System.out.println("testComputeTargetsScalesProportionally OK");
    }

    private static void testComputeTargetsOnlyIncludesExploredNodes() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node b = lattice.getRoot().to(1, 0).to(); // never updateNode'd

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, 10, 20);
        check(!targets.containsKey(b), "a node that was never explored should not get a top-up target");
        System.out.println("testComputeTargetsOnlyIncludesExploredNodes OK");
    }

    private static void testScheduleIncludesNodesBelowTargetAndRoutesToRoot() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        state.updateNode(a, 3, 7); // x=12, well below any reasonable target

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, 10, 20); // A's target = 48
        TopUpScheduler scheduler = new TopUpScheduler(targets);

        Schedule sched = scheduler.schedule(state, 0.0);
        check(sched.parents.containsKey(a), "A (x=12 < target=48) should be scheduled");
        check(sched.parents.get(lattice.getRoot()) == -1, "the path to A should resolve back to the root");
        System.out.println("testScheduleIncludesNodesBelowTargetAndRoutesToRoot OK");
    }

    private static void testScheduleExcludesIsolatedNodeAtOrAboveTarget() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node a = lattice.getRoot().to(0, 0).to(); // will be at-target, isolated (no descendants)
        Lattice.Node b = lattice.getRoot().to(1, 0).to(); // will be below-target

        state.updateNode(a, 3, 7);  // x=12
        state.updateNode(b, 1, 1);  // x=4

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, 10, 20); // A target=48, B target=16
        // manually push A up to/above its own target before scheduling (simulating "already topped up")
        state.updateNode(a, 30, 6); // x = 12+36 = 48, now at target

        TopUpScheduler scheduler = new TopUpScheduler(targets);
        Schedule sched = scheduler.schedule(state, 0.0);

        check(!sched.parents.containsKey(a), "A already at its target, with no dependents, should not be scheduled");
        check(sched.parents.containsKey(b), "B (x=4 < target=16) should still be scheduled");
        System.out.println("testScheduleExcludesIsolatedNodeAtOrAboveTarget OK");
    }

    private static void testThresholdParameterIsIgnored() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        Lattice.Node a = lattice.getRoot().to(0, 0).to();
        state.updateNode(a, 3, 7);

        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, 10, 20);
        TopUpScheduler scheduler = new TopUpScheduler(targets);

        Schedule low = scheduler.schedule(state, 0.0);
        Schedule high = scheduler.schedule(state, 999.0);
        check(low.parents.equals(high.parents), "the threshold parameter should have no effect on which nodes get scheduled");
        System.out.println("testThresholdParameterIsIgnored OK");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
