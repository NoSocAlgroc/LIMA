package lima;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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
import lima.lattice.PredicateRef;
import lima.sampler.LIMASampler;
import lima.sampler.Sample;
import lima.sampler.Sampler;
import lima.sampler.TopUpSampler;
import lima.scheduler.LIMAScheduler;
import lima.scheduler.Schedule;
import lima.scheduler.Scheduler;
import lima.scheduler.TopUpScheduler;
import lima.util.random.RandZ2;
import lima.util.stats.BetaDist;

/** Main controller: owns the dataset, its per-attribute predicate groups,
 * and the lattice belief state over them. */
public final class LIMA {
    /** Global mode: false = discover/print DCs, true = build/print the attribute relatedness matrix. */
    public static boolean ar = false;

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

        Map<Lattice.Edge, long[]> edgeResults = new HashMap<>();
        for (Map.Entry<Lattice.Node, Set<PredicateRef>> entry : sched.children.entrySet()) {
            Lattice.Node parent = entry.getKey();
            long[] parentCounts = result.counts.get(parent);
            if (parentCounts == null) {
                continue;
            }
            long parentA = parentCounts[0];
            if (parentA == 0) {
                continue;
            }

            for (PredicateRef ref : entry.getValue()) {
                Lattice.Edge edge = parent.to(ref.pg(), ref.p());
                long[] childCounts = result.counts.get(edge.to());
                if (childCounts == null) {
                    continue;
                }
                long childA = childCounts[0];
                if (childA > parentA) {
                    continue; // scaled top-up counts can break nesting
                }
                edgeResults.put(edge, new long[] {childA, parentA - childA});
            }
        }

        state.update(result.counts, edgeResults, devs);
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

    /** Render the Beta(a,b) of every explored lattice node and edge. */
    public String latticeStateText() {
        List<Lattice.Node> explored = new ArrayList<>();
        state.explored().forEachRemaining(explored::add);
        explored.sort(Comparator.comparingInt(Lattice.Node::size).thenComparing(this::nodeLabel));

        StringBuilder sb = new StringBuilder("NODES\n");
        for (Lattice.Node node : explored) {
            BetaDist d = state.getDist(node);
            sb.append(nodeLabel(node))
              .append(" a=").append(d.a)
              .append(" b=").append(d.b)
              .append('\n');
        }

        List<Lattice.Edge> edges = new ArrayList<>();
        state.exploredEdges().forEach(edges::add);
        edges.sort(Comparator
            .comparing((Lattice.Edge e) -> nodeLabel(e.fr()))
            .thenComparingInt(e -> e.pg)
            .thenComparingInt(e -> e.p));

        sb.append("\nEDGES\n");
        for (Lattice.Edge edge : edges) {
            BetaDist d = state.getEdgeDist(edge);
            sb.append(nodeLabel(edge.fr()))
              .append(" --(+").append(predicateText(edge.pg, edge.p)).append(")--> ")
              .append(nodeLabel(edge.to()))
              .append(" a=").append(d.a)
              .append(" b=").append(d.b)
              .append('\n');
        }
        return sb.toString();
    }

    private String nodeLabel(Lattice.Node node) {
        return node.preds().isEmpty() ? "root" : nodeText(node);
    }

    private String predicateText(int pg, int p) {
        String column = dataset.getColumns().get(pg);
        return column + PRED_OPS[p] + column;
    }

    /**
     * Attribute relatedness matrix over every sampled edge. Entry [x][y] is
     * the strongest -z seen for an edge that adds y on top of a node
     * containing x; zero means no such edge was visited.
     */
    public double[][] relatedness() {
        int npgs = predicateGroups.size();
        double[][] matrix = new double[npgs][npgs];
        for (double[] row : matrix) {
            Arrays.fill(row, Double.NEGATIVE_INFINITY);
        }

        for (Lattice.Edge edge : state.exploredEdges()) {
            Lattice.Node parent = edge.fr();
            double z = state.minSoundZ(edge);
            if (!Double.isFinite(z)) {
                continue;
            }

            double value = -z;
            for (int x : parent.preds().keySet()) {
                if (value > matrix[x][edge.pg]) {
                    matrix[x][edge.pg] = value;
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
