package lima.sampler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lima.data.domain.Domain;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.LatticeState;
import lima.scheduler.Schedule;

public final class LIMASampler implements Sampler {

    /**
     * Draw `n` samples from `domain` as `sched` calls for, returning the
     * observed counts per node.
     *
     * Estimates how often each predicate group will be evaluated,
     * precomputes the ones that cross the worthwhile-to-precompute
     * threshold, then DFS's the schedule tree to actually gather the (a, b)
     * counts per node.
     */
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

        return Sample.explore(domainSet, full, sched, state, predicateGroups, precomputed);
    }
}
