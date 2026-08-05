package lima.data.predicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;

/**
 * Deduplicates domain elements by their predicate behaviour.
 *
 * Elements that evaluate identically across every predicate group's minimal
 * variables ({@code pgEval}) are collapsed into one entry, with a count of
 * how many elements share it - e.g. 1000 domain elements with the same
 * behaviour become a single stored entry instead of 1000.
 */
public final class EviSet {
    private static final int WORD_BITS = 32;

    private final List<PredicateGroup> predicateGroups;
    private final int[] counts;                   // counts[p] = # domain elements sharing unique pattern p
    private final List<List<byte[]>> behaviours;   // [group][variable] -> byte[k], one flag per unique pattern

    public EviSet(List<PredicateGroup> predicateGroups, DomainSet<int[]> domain, DomainSubSet x) {
        this.predicateGroups = predicateGroups;
        int n = (x == null) ? domain.size() : x.indices().length;

        List<List<byte[]>> perGroup = evalPerGroup(predicateGroups, domain, x);
        int[] bitsPerGroup = bitsPerGroup(perGroup);
        List<byte[]> variables = flatten(perGroup);

        int numWords = (variables.size() + WORD_BITS - 1) / WORD_BITS;
        int[][] packed = packBits(variables, n, numWords);

        Map<PatternKey, Integer> grouped = countUnique(packed);
        int[][] patterns = new int[grouped.size()][];
        int[] counts = new int[grouped.size()];
        int p = 0;
        for (Map.Entry<PatternKey, Integer> entry : grouped.entrySet()) {
            patterns[p] = entry.getKey().words;
            counts[p] = entry.getValue();
            p++;
        }

        this.counts = counts;
        this.behaviours = buildBehaviours(bitsPerGroup, patterns);
    }

    /** The index list covering every unique behaviour. */
    public int[] fullIndex() {
        return PredicateGroup.range(counts.length);
    }

    /** Total number of domain elements represented by the unique behaviours in `index`. */
    public int totalCount(int[] index) {
        int total = 0;
        for (int i : index) {
            total += counts[i];
        }
        return total;
    }

    /**
     * Restrict `index` (a list of unique-behaviour indices) to those
     * satisfying predicate `predIdx` of predicate group `pgIdx`.
     *
     * Gets that predicate group's variables restricted to `index`, uses its
     * `predPositions` to find which of those satisfy the predicate, and
     * maps the resulting local positions back through `index`.
     */
    public int[] filterByPredicate(int[] index, int pgIdx, int predIdx) {
        List<byte[]> groupVariables = behaviours.get(pgIdx);
        List<byte[]> restricted = new ArrayList<>(groupVariables.size());
        for (byte[] var : groupVariables) {
            byte[] sub = new byte[index.length];
            for (int i = 0; i < index.length; i++) {
                sub[i] = var[index[i]];
            }
            restricted.add(sub);
        }

        Map<Integer, int[]> predsPositions = predicateGroups.get(pgIdx).predPositions(restricted, Set.of(predIdx));
        int[] localIdx = predsPositions.get(predIdx);

        int[] result = new int[localIdx.length];
        for (int i = 0; i < localIdx.length; i++) {
            result[i] = index[localIdx[i]];
        }
        return result;
    }

    // ------------------------------------------------------------------
    // Construction helpers
    // ------------------------------------------------------------------

    private static List<List<byte[]>> evalPerGroup(
        List<PredicateGroup> predicateGroups, DomainSet<int[]> domain, DomainSubSet x
    ) {
        List<List<byte[]>> perGroup = new ArrayList<>(predicateGroups.size());
        for (PredicateGroup pg : predicateGroups) {
            perGroup.add(pg.pgEval(domain, x));
        }
        return perGroup;
    }

    /** How many variables (bits) each predicate group contributed, so the
     * flattened bits can later be split back up per group. */
    private static int[] bitsPerGroup(List<List<byte[]>> perGroup) {
        int[] result = new int[perGroup.size()];
        for (int g = 0; g < perGroup.size(); g++) {
            result[g] = perGroup.get(g).size();
        }
        return result;
    }

    /** All pgEval variables across every predicate group, flattened into a
     * single list (one entry per bit of behaviour). */
    private static List<byte[]> flatten(List<List<byte[]>> perGroup) {
        List<byte[]> flat = new ArrayList<>();
        for (List<byte[]> variables : perGroup) {
            flat.addAll(variables);
        }
        return flat;
    }

    /** Pack `variables` (m columns of length n) into (n, numWords) int
     * words directly, without ever materializing the (n, m) 0/1 matrix:
     * bit j of a record lives at bit (j % 32) of word (j / 32). */
    private static int[][] packBits(List<byte[]> variables, int n, int numWords) {
        int[][] packed = new int[n][numWords];
        for (int j = 0; j < variables.size(); j++) {
            byte[] column = variables.get(j);
            int wordIdx = j / WORD_BITS;
            int mask = 1 << (j % WORD_BITS);
            for (int i = 0; i < n; i++) {
                if (column[i] != 0) {
                    packed[i][wordIdx] |= mask;
                }
            }
        }
        return packed;
    }

    /** Distinct predicate-behaviour patterns and how many domain elements
     * share each one, in first-occurrence order (unlike np.unique, this
     * doesn't sort - nothing downstream depends on pattern order, only on
     * patterns/counts/behaviours staying aligned to the same index). */
    private static Map<PatternKey, Integer> countUnique(int[][] packed) {
        Map<PatternKey, Integer> grouped = new LinkedHashMap<>();
        for (int[] words : packed) {
            grouped.merge(new PatternKey(words), 1, Integer::sum);
        }
        return grouped;
    }

    /** Split each unique pattern's packed words back into per-(group,
     * variable) 0/1 flags, one array of length k (the number of unique
     * patterns) per variable - the inverse of packBits, run only over the
     * deduplicated patterns rather than every original domain element. */
    private static List<List<byte[]>> buildBehaviours(int[] bitsPerGroup, int[][] patterns) {
        int k = patterns.length;
        byte[][][] byGroup = new byte[bitsPerGroup.length][][];
        for (int g = 0; g < bitsPerGroup.length; g++) {
            byGroup[g] = new byte[bitsPerGroup[g]][];
            for (int v = 0; v < bitsPerGroup[g]; v++) {
                byGroup[g][v] = new byte[k];
            }
        }

        for (int p = 0; p < k; p++) {
            int[] words = patterns[p];
            int bit = 0;
            for (int g = 0; g < bitsPerGroup.length; g++) {
                for (int v = 0; v < bitsPerGroup[g]; v++) {
                    int value = (words[bit / WORD_BITS] >>> (bit % WORD_BITS)) & 1;
                    byGroup[g][v][p] = (byte) value;
                    bit++;
                }
            }
        }

        List<List<byte[]>> result = new ArrayList<>(bitsPerGroup.length);
        for (byte[][] group : byGroup) {
            result.add(Arrays.asList(group));
        }
        return result;
    }

    /** Content-hashable wrapper around a packed record's words - plain
     * int[] has reference equality, so it can't be used as a map key
     * directly. */
    private static final class PatternKey {
        final int[] words;

        PatternKey(int[] words) {
            this.words = words;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof PatternKey other && Arrays.equals(words, other.words);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(words);
        }
    }
}
