package lima.sampler;

import java.util.List;

import lima.data.domain.Domain;
import lima.data.domain.DomainSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.LatticeState;
import lima.scheduler.Schedule;

/** Abstract: draws n samples from a domain following a schedule. */
public interface Sampler {
    Sample sample(
        Schedule sched, Domain<? extends DomainSet<int[]>> domain, List<PredicateGroup> predicateGroups,
        LatticeState state, int n
    );
}
