package lima.scheduler;

import java.util.Map;
import java.util.Set;

import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.lattice.PredicateRef;

public final class LIMAScheduler implements Scheduler {

    /** Build this round's sampling tree from the nodes adjacent to
     * state.candidateEdges() whose nodeGrad exceeds `threshold`. */
    @Override
    public Schedule schedule(LatticeState state, double threshold) {
        Set<Lattice.Node> nodes = Schedule.adjacentNodes(state.candidateEdges());
        nodes = Schedule.nodesAboveThreshold(nodes, state, threshold);
        Map<Lattice.Node, Integer> tree = Schedule.buildTree(nodes, state);
        Map<Lattice.Node, Set<PredicateRef>> children = Schedule.invertTree(tree, state);
        return new Schedule(tree, children);
    }
}
