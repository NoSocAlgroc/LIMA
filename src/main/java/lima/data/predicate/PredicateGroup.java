package lima.data.predicate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.dataset.DType;
import lima.data.dataset.Dataset;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;

/**
 * Abstract group of predicates, defined over a single dataset column,
 * evaluated over pairs of dataset rows.
 */
public abstract class PredicateGroup {
    protected final Dataset dataset;
    protected final String column;

    protected PredicateGroup(Dataset dataset, String column) {
        this.dataset = dataset;
        this.column = column;
    }

    /** Number of predicates this group evaluates (the size of eval's default preds). */
    public abstract int numPredicates();

    /**
     * Compute this group's minimal set of underlying boolean variables over
     * the pairs at positions {@code x} of {@code domain} (e.g. just `same`
     * for an unordered group, `same`/`less` for an ordered one), aligned
     * with {@code x}'s positions (or the full domain if {@code x} is null).
     *
     * {@code eval} derives its full predicate set from these variables.
     * Each variable is a 0/1 flag per pair, stored as a byte.
     */
    public abstract List<byte[]> pgEval(DomainSet<int[]> domain, DomainSubSet x);

    /**
     * From this group's {@code pgEval} variables, the local index arrays
     * (positions into those variables, not into any DomainSet) of the
     * elements satisfying each requested predicate. {@code preds == null}
     * means every predicate this group has.
     *
     * This is the part of {@code eval} that doesn't need a DomainSet/
     * DomainSubSet at all - just the variable arrays.
     */
    public abstract Map<Integer, int[]> predPositions(List<byte[]> variables, Set<Integer> preds);

    /**
     * Build the final per-predicate DomainSubSets from {@code predPositions}'s
     * local index arrays, mapped through the domain-level {@code positions}
     * this evaluation covered.
     */
    public static Map<Integer, DomainSubSet> makeSubsets(
        int n, int[] positions, Map<Integer, int[]> predsPositions
    ) {
        Map<Integer, DomainSubSet> result = new LinkedHashMap<>();
        for (Map.Entry<Integer, int[]> entry : predsPositions.entrySet()) {
            int[] localIdx = entry.getValue();
            int[] mapped = new int[localIdx.length];
            for (int i = 0; i < localIdx.length; i++) {
                mapped[i] = positions[localIdx[i]];
            }
            result.put(entry.getKey(), DomainSubSet.make(n, mapped));
        }
        return result;
    }

    /**
     * Evaluate the predicate group over the pairs at positions {@code x} of
     * {@code domain}, restricted to the predicates named in {@code preds}.
     *
     * {@code x == null} means the full domain - implementations should use
     * {@code domain.getFull()} for that case rather than materializing a
     * full index array and indexing with it. {@code preds == null} means
     * every predicate this group has.
     *
     * Returns a map from each requested predicate to the DomainSubSet
     * satisfying it. Implementations should run pgEval, then predPositions,
     * then makeSubsets, in that order.
     */
    public abstract Map<Integer, DomainSubSet> eval(DomainSet<int[]> domain, DomainSubSet x, Set<Integer> preds);

    /**
     * One UnorderedPG (categoric columns) or OrderedPG (numeric columns)
     * per column of {@code dataset}, in column order.
     */
    public static List<PredicateGroup> buildPredicateGroups(Dataset dataset) {
        List<PredicateGroup> groups = new ArrayList<>();
        for (String column : dataset.getColumns()) {
            DType dtype = dataset.getDType(column);
            if (dtype == DType.CATEGORY) {
                groups.add(new UnorderedPG(dataset, column));
            } else {
                groups.add(new OrderedPG(dataset, column));
            }
        }
        return groups;
    }

    // ------------------------------------------------------------------
    // Helpers shared by UnorderedPG/OrderedPG
    // ------------------------------------------------------------------

    /** Local indices where `flags[i] == target`, in ascending order. */
    protected static int[] where(byte[] flags, byte target) {
        int count = 0;
        for (byte f : flags) {
            if (f == target) {
                count++;
            }
        }
        int[] result = new int[count];
        int j = 0;
        for (int i = 0; i < flags.length; i++) {
            if (flags[i] == target) {
                result[j++] = i;
            }
        }
        return result;
    }

    protected static int[] range(int n) {
        int[] result = new int[n];
        for (int i = 0; i < n; i++) {
            result[i] = i;
        }
        return result;
    }
}
