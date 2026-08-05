package lima.sampler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lima.data.domain.Domain;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.scheduler.Schedule;

/**
 * Sampler for the "top-up" phase after the domain grows: draws from
 * whatever domain it's given (typically a difference-domain covering only
 * the newly-added region) and otherwise reuses the same pipeline as
 * LIMASampler (estimateReach/partitionPredicateGroups/explore) for the raw
 * per-node counts. The one new step is scaling a node's round counts down
 * when they'd overshoot its remaining need (target - current total), so a
 * node's accumulated sample count never overshoots its precomputed target
 * even though it may keep getting visited as a structural ancestor of
 * still-active descendants.
 */
public final class TopUpSampler implements Sampler {
    private final Map<Lattice.Node, Long> targets;

    public TopUpSampler(Map<Lattice.Node, Long> targets) {
        this.targets = targets;
    }

    @Override
    public Sample sample(
        Schedule sched, Domain<? extends DomainSet<int[]>> domain, List<PredicateGroup> predicateGroups,
        LatticeState state, int n
    ) {
        double[] counts = Sample.estimateReach(sched, state, n, predicateGroups.size());
        List<Integer> precomputedIds = Sample.partitionPredicateGroups(counts, n).getKey();

        Map.Entry<DomainSet<int[]>, DomainSubSet> domainAndFull = Sample.makeFullDomainSet(domain, n);
        DomainSet<int[]> domainSet = domainAndFull.getKey();
        DomainSubSet full = domainAndFull.getValue();

        Map<Integer, PredicateGroup> toPrecompute = new HashMap<>();
        for (int pgId : precomputedIds) {
            toPrecompute.put(pgId, predicateGroups.get(pgId));
        }
        Map<Integer, List<DomainSubSet>> precomputed = Sample.evaluatePredicateGroups(toPrecompute, domainSet, full);

        Sample raw = Sample.explore(domainSet, full, sched, state, predicateGroups, precomputed);
        return scaleToRemainingNeed(raw, state);
    }

    private Sample scaleToRemainingNeed(Sample raw, LatticeState state) {
        Map<Lattice.Node, long[]> scaled = new HashMap<>();
        for (Map.Entry<Lattice.Node, long[]> entry : raw.counts.entrySet()) {
            scaled.put(entry.getKey(), scaleOne(entry.getKey(), entry.getValue(), state));
        }
        return new Sample(scaled);
    }

    private long[] scaleOne(Lattice.Node node, long[] ab, LatticeState state) {
        long total = ab[0] + ab[1];
        if (total == 0) {
            return ab;
        }

        Long target = targets.get(node);
        if (target == null) {
            return ab; // not one of the nodes being topped up - just a structural ancestor
        }

        long currentTotal = state.getDist(node).a + state.getDist(node).b;
        long remainingNeeded = Math.max(0, target - currentTotal);
        if (total <= remainingNeeded) {
            return ab;
        }

        long scaledA = Math.round(ab[0] * (remainingNeeded / (double) total));
        long scaledB = remainingNeeded - scaledA; // derived, not independently rounded, so a+b == remainingNeeded exactly
        return new long[] {scaledA, scaledB};
    }
}
