package lima;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import lima.data.dataset.Dataset;
import lima.data.domain.Domain;
import lima.data.domain.TPDifferenceDomain;
import lima.data.domain.TPDomain;
import lima.data.domain.TPDomainSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.lattice.LIMALatticeState;
import lima.sampler.LIMASampler;
import lima.sampler.Sample;
import lima.sampler.Sampler;
import lima.sampler.TopUpSampler;
import lima.scheduler.LIMAScheduler;
import lima.scheduler.Schedule;
import lima.scheduler.Scheduler;
import lima.scheduler.TopUpScheduler;
import lima.util.random.RandZ2;

/** Main controller: owns the dataset, its per-attribute predicate groups,
 * and the lattice belief state over them. */
public final class LIMA {
    private static final String[] PRED_OPS = {"=", "!=", "<", ">"}; // predicate id -> operator symbol, shared by Unordered/OrderedPG

    private final Dataset dataset;
    private final List<PredicateGroup> predicateGroups;
    private Domain<TPDomainSet> domain;
    private final Lattice lattice;
    private final LatticeState state;
    private Scheduler scheduler;
    private Sampler sampler;
    private final double approx; // scheduler threshold for filtering nodes (nodeGrad cutoff)
    private final double devs;   // soundness test threshold, in z-score standard deviations
    private int currentRange;    // active row range [0, currentRange) - grows via expand()
    private final Random random; // shared source of all sampling randomness

    public LIMA(Dataset dataset, Scheduler scheduler, Sampler sampler, double approx, double devs, int initialRange) {
        this(dataset, scheduler, sampler, approx, devs, initialRange, new Random());
    }

    public LIMA(Dataset dataset, Scheduler scheduler, Sampler sampler, double approx, double devs, int initialRange, long seed) {
        this(dataset, scheduler, sampler, approx, devs, initialRange, new Random(seed));
    }

    private LIMA(Dataset dataset, Scheduler scheduler, Sampler sampler, double approx, double devs, int initialRange, Random random) {
        this.dataset = dataset;
        this.predicateGroups = PredicateGroup.buildPredicateGroups(dataset);
        this.random = random;

        this.currentRange = initialRange;
        this.domain = new TPDomain(initialRange, new RandZ2(64, random));
        int[] pgs = new int[predicateGroups.size()];
        for (int i = 0; i < pgs.length; i++) {
            pgs[i] = predicateGroups.get(i).numPredicates();
        }
        this.lattice = new Lattice(pgs);
        this.state = new LIMALatticeState(lattice);

        this.scheduler = scheduler;
        this.sampler = sampler;

        this.approx = approx;
        this.devs = devs;
    }

    public LIMA(Dataset dataset, Scheduler scheduler, Sampler sampler, double approx, double devs) {
        this(dataset, scheduler, sampler, approx, devs, dataset.numRows());
    }

    public LIMA(Dataset dataset, double approx) {
        this(dataset, new LIMAScheduler(), new LIMASampler(), approx, 4.0, dataset.numRows());
    }

    public LIMA(Dataset dataset, double approx, int initialRange) {
        this(dataset, new LIMAScheduler(), new LIMASampler(), approx, 4.0, initialRange);
    }

    public LIMA(Dataset dataset, double approx, int initialRange, long seed) {
        this(dataset, new LIMAScheduler(), new LIMASampler(), approx, 4.0, initialRange, seed);
    }

    public int getStepSize(int iteration) {
        return 1 << (10 + Math.min(iteration, 10));
    }

    /** Run one round: schedule, sample, and update. Returns true when the
     * schedule is empty (nothing left to explore). */
    public boolean step(int iteration) {
        Schedule sched = scheduler.schedule(state, approx);
        if (sched.parents.isEmpty()) {
            return true;
        }

        int n = getStepSize(iteration);
        Sample result = sampler.sample(sched, domain, predicateGroups, state, n);
        state.update(result.counts, devs);
        return false;
    }

    /**
     * Grow the active row range by {@code additionalRows} and switch to the
     * top-up phase: each explored node's target sample count is rescaled to
     * the new domain size, and further samples are drawn only from the newly
     * added region.
     */
    public void expand(int additionalRows) {
        if (additionalRows <= 0) {
            throw new IllegalArgumentException("additionalRows must be positive, got " + additionalRows);
        }
        int newRange = currentRange + additionalRows;
        Map<Lattice.Node, Long> targets = TopUpScheduler.computeTargets(state, currentRange, newRange);

        this.scheduler = new TopUpScheduler(targets);
        this.sampler = new TopUpSampler(targets);
        this.domain = new TPDifferenceDomain(currentRange, newRange, new RandZ2(64, random));
        this.currentRange = newRange;
    }

    /** Sound edges whose to-node has never seen a positive observation
     * (BetaDist.a == 1, i.e. still exactly the uninformative prior's a). */
    public Set<Lattice.Node> deadEnds() {
        Set<Lattice.Node> result = new HashSet<>();
        for (Lattice.Edge edge : state.soundEdges()) {
            if (state.getDist(edge.to()).a == 1) {
                result.add(edge.to());
            }
        }
        return result;
    }

    /** Render node n as e.g. "!(colA=colA & colB!=colB)": the negated
     * conjunction of its predicates, each written as column&lt;op&gt;column. */
    public String nodeText(Lattice.Node n) {
        StringBuilder sb = new StringBuilder("!(");
        boolean first = true;
        for (Map.Entry<Integer, Integer> entry : n.preds().entrySet()) {
            String column = dataset.getColumns().get(entry.getKey());
            if (!first) {
                sb.append(" & ");
            }
            sb.append(column).append(PRED_OPS[entry.getValue()]).append(column);
            first = false;
        }
        return sb.append(")").toString();
    }

    /** Text rendering of every dead-end node from deadEnds(). */
    public List<String> deadEndsText() {
        List<String> result = new ArrayList<>();
        for (Lattice.Node n : deadEnds()) {
            result.add(nodeText(n));
        }
        return result;
    }

    /**
     * Attribute relatedness matrix over every edge between explored nodes.
     * Entry [x][y] is the strongest -z seen for an edge that adds y on top
     * of a node containing x; zero means no such edge was visited.
     */
    public double[][] relatedness() {
        int npgs = predicateGroups.size();
        double[][] matrix = new double[npgs][npgs];
        for (double[] row : matrix) {
            Arrays.fill(row, Double.NEGATIVE_INFINITY);
        }

        Set<Lattice.Node> explored = new HashSet<>();
        Iterator<Lattice.Node> it = state.explored();
        while (it.hasNext()) {
            explored.add(it.next());
        }

        for (Lattice.Node parent : explored) {
            if (parent.preds().isEmpty()) {
                continue;
            }
            for (int pg = 0; pg < lattice.npgs(); pg++) {
                if (parent.contains(pg)) {
                    continue;
                }
                for (int p = 0; p < lattice.pgs(pg); p++) {
                    Lattice.Edge edge = parent.to(pg, p);
                    if (!explored.contains(edge.to())) {
                        continue;
                    }
                    double value = -state.minSoundZ(edge);
                    for (int x : parent.preds().keySet()) {
                        if (value > matrix[x][pg]) {
                            matrix[x][pg] = value;
                        }
                    }
                }
            }
        }

        for (double[] row : matrix) {
            for (int j = 0; j < row.length; j++) {
                if (row[j] == Double.NEGATIVE_INFINITY) {
                    row[j] = 0.0;
                }
            }
        }
        return matrix;
    }
}
