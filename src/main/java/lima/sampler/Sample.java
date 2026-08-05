package lima.sampler;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.domain.Domain;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;
import lima.data.predicate.PredicateGroup;
import lima.lattice.Lattice;
import lima.lattice.LatticeState;
import lima.lattice.PredicateRef;
import lima.scheduler.Schedule;

/** Observed (successes, failures) counts per lattice node, from one sampling round. */
public final class Sample {
    public final Map<Lattice.Node, long[]> counts;

    public Sample() {
        this(new HashMap<>());
    }

    public Sample(Map<Lattice.Node, long[]> counts) {
        this.counts = counts;
    }

    /** Instantiate a size-n DomainSet from `domain`, along with the DomainSubSet covering all of it. */
    public static Map.Entry<DomainSet<int[]>, DomainSubSet> makeFullDomainSet(
        Domain<? extends DomainSet<int[]>> domain, int n
    ) {
        DomainSet<int[]> domainSet = domain.makeDomainSet(n);
        return Map.entry(domainSet, domainSet.full());
    }

    /**
     * Estimate, per predicate group, the total number of domain elements
     * expected to reach nodes in the schedule tree through that group.
     *
     * Walks `sched`'s tree from the root (reach = n); for each child edge
     * (pg, p) of a node, the reach estimate for that child is
     * `reachAtNode * state.getDist(node).mean`, which is added to
     * `counts[pg]` and propagated as the reach for that child in turn.
     */
    public static double[] estimateReach(Schedule sched, LatticeState state, int n, int npgs) {
        double[] counts = new double[npgs];
        Lattice.Node root = state.lattice().getRoot();

        Deque<Map.Entry<Lattice.Node, Double>> stack = new ArrayDeque<>();
        stack.push(Map.entry(root, (double) n));

        while (!stack.isEmpty()) {
            Map.Entry<Lattice.Node, Double> top = stack.pop();
            Lattice.Node node = top.getKey();
            double reach = top.getValue();
            double mean = state.getDist(node).mean;

            for (PredicateRef ref : sched.children.getOrDefault(node, Set.of())) {
                double estimate = reach * mean;
                counts[ref.pg()] += estimate;
                Lattice.Node child = node.to(ref.pg(), ref.p()).to();
                stack.push(Map.entry(child, estimate));
            }
        }
        return counts;
    }

    /**
     * Decide which predicate groups to precompute (evaluate over the whole
     * domain once, memoize, and AND with the DomainSubSet at each node)
     * versus filter on demand.
     *
     * `counts[pg]` is treated as an estimate of how many times pg will need
     * to be evaluated; if that exceeds `n * factor`, it's worth precomputing.
     *
     * Returns (precomputed, onDemand): lists of predicate group indices.
     */
    public static Map.Entry<List<Integer>, List<Integer>> partitionPredicateGroups(double[] counts, int n, double factor) {
        double threshold = n * factor;
        List<Integer> precomputed = new ArrayList<>();
        List<Integer> onDemand = new ArrayList<>();
        for (int pg = 0; pg < counts.length; pg++) {
            if (counts[pg] > threshold) {
                precomputed.add(pg);
            } else {
                onDemand.add(pg);
            }
        }
        return Map.entry(precomputed, onDemand);
    }

    public static Map.Entry<List<Integer>, List<Integer>> partitionPredicateGroups(double[] counts, int n) {
        return partitionPredicateGroups(counts, n, 0.01);
    }

    /**
     * Evaluate every predicate of every predicate group in `pgs` over `x`.
     *
     * `pgs` is keyed by predicate group id. Returns a map with the same
     * keys, each mapping to a list with one DomainSubSet per predicate of
     * that group (2 for UnorderedPG, 4 for OrderedPG), ordered by predicate
     * id - i.e. exactly the format filterDomain's `precomputed` argument
     * expects.
     */
    public static Map<Integer, List<DomainSubSet>> evaluatePredicateGroups(
        Map<Integer, PredicateGroup> pgs, DomainSet<int[]> domain, DomainSubSet x
    ) {
        Map<Integer, List<DomainSubSet>> result = new HashMap<>();
        for (Map.Entry<Integer, PredicateGroup> entry : pgs.entrySet()) {
            Map<Integer, DomainSubSet> evaluated = entry.getValue().eval(domain, x, null);

            List<Integer> sortedPreds = new ArrayList<>(evaluated.keySet());
            Collections.sort(sortedPreds);

            List<DomainSubSet> ordered = new ArrayList<>(sortedPreds.size());
            for (int p : sortedPreds) {
                ordered.add(evaluated.get(p));
            }
            result.put(entry.getKey(), ordered);
        }
        return result;
    }

    /**
     * Evaluate a single predicate `p` of `pg` over `x`.
     *
     * Used when exploring the lattice tree and the added predicate wasn't
     * among the precomputed ones.
     */
    public static DomainSubSet evaluatePredicate(PredicateGroup pg, int p, DomainSet<int[]> domain, DomainSubSet x) {
        return pg.eval(domain, x, Set.of(p)).get(p);
    }

    /**
     * Filter `x` by predicate `p` of predicate group `pg` (with id `pgId`).
     *
     * If `pgId` was precomputed (present in `precomputed`, e.g. via
     * evaluatePredicateGroups), just AND `x` with the precomputed subset
     * for `p` instead of evaluating the predicate again.
     */
    public static DomainSubSet filterDomain(
        DomainSubSet x, int pgId, PredicateGroup pg, int p, DomainSet<int[]> domain,
        Map<Integer, List<DomainSubSet>> precomputed
    ) {
        if (precomputed.containsKey(pgId)) {
            return x.and(precomputed.get(pgId).get(p));
        }
        return evaluatePredicate(pg, p, domain, x);
    }

    /**
     * DFS the schedule tree from `node`/`x`, recording (a, b) = (size(x),
     * size(domain) - size(x)) - the new BetaDist observation for each node
     * - into `result`, then recursing into each child after filtering `x`
     * by that child edge's (pg, p).
     */
    private static void exploreDFS(
        Lattice.Node node, DomainSubSet x, DomainSet<int[]> domain, Schedule sched,
        List<PredicateGroup> predicateGroups, Map<Integer, List<DomainSubSet>> precomputed, Sample result
    ) {
        long a = x.size();
        long b = domain.size() - a;
        result.counts.put(node, new long[] {a, b});

        for (PredicateRef ref : sched.children.getOrDefault(node, Set.of())) {
            Lattice.Node childNode = node.to(ref.pg(), ref.p()).to();
            DomainSubSet childX = filterDomain(x, ref.pg(), predicateGroups.get(ref.pg()), ref.p(), domain, precomputed);
            exploreDFS(childNode, childX, domain, sched, predicateGroups, precomputed, result);
        }
    }

    /** Runner: DFS `sched`'s tree starting at the lattice root with the full DomainSubSet `full`. */
    public static Sample explore(
        DomainSet<int[]> domain, DomainSubSet full, Schedule sched, LatticeState state,
        List<PredicateGroup> predicateGroups, Map<Integer, List<DomainSubSet>> precomputed
    ) {
        Sample result = new Sample();
        Lattice.Node root = state.lattice().getRoot();
        exploreDFS(root, full, domain, sched, predicateGroups, precomputed, result);
        return result;
    }
}
