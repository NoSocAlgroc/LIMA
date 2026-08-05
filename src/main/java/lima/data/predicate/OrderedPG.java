package lima.data.predicate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lima.data.dataset.Column;
import lima.data.dataset.DType;
import lima.data.dataset.Dataset;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;

/**
 * Predicate group for ordered (numeric) columns: how the two rows of a pair
 * compare in `column`.
 *
 * preds 0 -> equal, 1 -> not equal, 2 -> left &lt; right, 3 -> left &gt;
 * right (defaults to computing all four).
 */
public final class OrderedPG extends PredicateGroup {
    private final Column values;
    private final boolean isInteger;

    public OrderedPG(Dataset dataset, String column) {
        super(dataset, column);
        this.values = dataset.getColumn(column);
        if (values.dtype() == DType.CATEGORY) {
            throw new IllegalArgumentException(
                "OrderedPG requires a numeric (INTEGER or DOUBLE) column, got CATEGORY for '" + column + "'");
        }
        this.isInteger = values.dtype() == DType.INTEGER;
    }

    @Override
    public int numPredicates() {
        return 4;
    }

    @Override
    public List<byte[]> pgEval(DomainSet<int[]> domain, DomainSubSet x) {
        int[][] pairs = (x == null) ? domain.getFull() : domain.get(x.indices());

        byte[] same = new byte[pairs.length];
        byte[] less = new byte[pairs.length];
        if (isInteger) {
            for (int i = 0; i < pairs.length; i++) {
                long left = values.getLong(pairs[i][0]);
                long right = values.getLong(pairs[i][1]);
                same[i] = (byte) (left == right ? 1 : 0);
                less[i] = (byte) (left < right ? 1 : 0);
            }
        } else {
            for (int i = 0; i < pairs.length; i++) {
                double left = values.getDouble(pairs[i][0]);
                double right = values.getDouble(pairs[i][1]);
                same[i] = (byte) (left == right ? 1 : 0);
                less[i] = (byte) (left < right ? 1 : 0);
            }
        }
        return List.of(same, less);
    }

    @Override
    public Map<Integer, int[]> predPositions(List<byte[]> variables, Set<Integer> preds) {
        Set<Integer> requested = preds != null ? preds : Set.of(0, 1, 2, 3);
        byte[] same = variables.get(0);
        byte[] less = variables.get(1);

        Map<Integer, int[]> result = new LinkedHashMap<>();
        if (requested.contains(0)) {
            result.put(0, where(same, (byte) 1));
        }
        if (requested.contains(1)) {
            result.put(1, where(same, (byte) 0));
        }
        if (requested.contains(2)) {
            result.put(2, where(less, (byte) 1));
        }
        if (requested.contains(3)) {
            result.put(3, whereGreater(same, less));
        }
        return result;
    }

    /** not (less or same), i.e. left > right. */
    private static int[] whereGreater(byte[] same, byte[] less) {
        int count = 0;
        for (int i = 0; i < same.length; i++) {
            if (same[i] == 0 && less[i] == 0) {
                count++;
            }
        }
        int[] result = new int[count];
        int j = 0;
        for (int i = 0; i < same.length; i++) {
            if (same[i] == 0 && less[i] == 0) {
                result[j++] = i;
            }
        }
        return result;
    }

    @Override
    public Map<Integer, DomainSubSet> eval(DomainSet<int[]> domain, DomainSubSet x, Set<Integer> preds) {
        int n = domain.size();
        int[] positions = (x == null) ? range(n) : x.indices();

        List<byte[]> variables = pgEval(domain, x);
        Map<Integer, int[]> predsPositions = predPositions(variables, preds);
        return makeSubsets(n, positions, predsPositions);
    }
}
