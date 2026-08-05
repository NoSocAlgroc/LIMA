package lima.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.lattice.PredicateRef;
import lima.util.stats.BetaDist;

/**
 * Scheduler for the "top-up" phase after the domain grows: instead of
 * discovering new nodes via nodeGrad/candidateEdges (LIMAScheduler's job),
 * it revisits every node that was already explored before the domain grew
 * and keeps scheduling it until its sample count (a+b) reaches a
 * precomputed target - saturation by sample count rather than by
 * distribution variance, which only governs the very first (unexpanded)
 * pass.
 */
public final class TopUpScheduler implements Scheduler {
    private final Map<Lattice.Node, Long> targets;

    public TopUpScheduler(Map<Lattice.Node, Long> targets) {
        this.targets = targets;
    }

    /**
     * Each currently-explored node's target sample count after growing the
     * domain from oldRange to newRange: target = round(x * newRange^2 /
     * oldRange^2), where x is that node's current a+b. Meant to be shared
     * by a TopUpScheduler/TopUpSampler pair for one expansion phase, so
     * both agree on the same per-node goal.
     */
    public static Map<Lattice.Node, Long> computeTargets(LatticeState state, int oldRange, int newRange) {
        double scale = ((double) newRange * newRange) / ((double) oldRange * oldRange);

        Map<Lattice.Node, Long> targets = new HashMap<>();
        Iterator<Lattice.Node> it = state.explored();
        while (it.hasNext()) {
            Lattice.Node node = it.next();
            BetaDist dist = state.getDist(node);
            long x = dist.a + dist.b;
            targets.put(node, Math.round(x * scale));
        }
        return targets;
    }

    /** `threshold` is unused - which nodes still need samples is decided entirely by the precomputed targets. */
    @Override
    public Schedule schedule(LatticeState state, double threshold) {
        Set<Lattice.Node> nodes = nodesBelowTarget(state);
        Map<Lattice.Node, Integer> tree = Schedule.buildTree(nodes, state);
        Map<Lattice.Node, Set<PredicateRef>> children = Schedule.invertTree(tree, state);
        return new Schedule(tree, children);
    }

    private Set<Lattice.Node> nodesBelowTarget(LatticeState state) {
        Set<Lattice.Node> result = new HashSet<>();
        for (Map.Entry<Lattice.Node, Long> entry : targets.entrySet()) {
            BetaDist dist = state.getDist(entry.getKey());
            if (dist.a + dist.b < entry.getValue()) {
                result.add(entry.getKey());
            }
        }
        return result;
    }
}
