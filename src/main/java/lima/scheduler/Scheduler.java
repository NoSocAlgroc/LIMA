package lima.scheduler;

import lima.lattice.LatticeState;

/** Abstract: decides this round's sampling tree from the lattice state. */
public interface Scheduler {
    Schedule schedule(LatticeState state, double threshold);
}
