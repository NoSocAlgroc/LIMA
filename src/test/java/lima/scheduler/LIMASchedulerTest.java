package lima.scheduler;

import java.util.Map;
import java.util.Set;

import lima.lattice.Lattice;
import lima.lattice.LIMALatticeState;
import lima.lattice.PredicateRef;

/** Standalone smoke tests for LIMAScheduler - run directly, no test framework wired up yet. */
public final class LIMASchedulerTest {

    public static void main(String[] args) {
        testHighThresholdProducesEmptySchedule();
        testLowThresholdProducesNonEmptySchedule();
        testScheduleMatchesManualPipeline();
        System.out.println("All LIMAScheduler tests passed.");
    }

    private static void testHighThresholdProducesEmptySchedule() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        LIMAScheduler scheduler = new LIMAScheduler();

        // no achievable nodeGrad value (always <= 0.25 for a Beta(1,1) prior,
        // and only shrinks with more evidence) can exceed this
        Schedule sched = scheduler.schedule(state, 1000.0);

        check(sched.parents.isEmpty(), "an unreachably high threshold should exclude every node");
        check(sched.children.isEmpty(), "an unreachably high threshold should produce no children either");
        System.out.println("testHighThresholdProducesEmptySchedule OK");
    }

    private static void testLowThresholdProducesNonEmptySchedule() {
        Lattice lattice = new Lattice(new int[] {2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        LIMAScheduler scheduler = new LIMAScheduler();

        Schedule sched = scheduler.schedule(state, 0.0);

        check(!sched.parents.isEmpty(), "a threshold of 0 should include the freshly-seeded candidate nodes");
        check(sched.parents.containsKey(lattice.getRoot()), "the tree should always resolve back to the root");
        System.out.println("testLowThresholdProducesNonEmptySchedule OK");
    }

    private static void testScheduleMatchesManualPipeline() {
        Lattice lattice = new Lattice(new int[] {2, 2, 2});
        LIMALatticeState state = new LIMALatticeState(lattice);
        // give one node enough evidence to meaningfully change which nodes clear the threshold
        state.updateNode(lattice.getRoot().to(0, 0).to(), 4999, 4999);
        double threshold = 0.1;

        LIMAScheduler scheduler = new LIMAScheduler();
        Schedule actual = scheduler.schedule(state, threshold);

        Set<Lattice.Node> nodes = Schedule.adjacentNodes(state.candidateEdges());
        nodes = Schedule.nodesAboveThreshold(nodes, state, threshold);
        Map<Lattice.Node, Integer> expectedTree = Schedule.buildTree(nodes, state);
        Map<Lattice.Node, Set<PredicateRef>> expectedChildren = Schedule.invertTree(expectedTree, state);

        check(actual.parents.equals(expectedTree),
            "LIMAScheduler.schedule's tree should exactly match chaining adjacentNodes/nodesAboveThreshold/buildTree by hand");
        check(actual.children.equals(expectedChildren),
            "LIMAScheduler.schedule's children map should exactly match invertTree of that same tree");
        System.out.println("testScheduleMatchesManualPipeline OK");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
