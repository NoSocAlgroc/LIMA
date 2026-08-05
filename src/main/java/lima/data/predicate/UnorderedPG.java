package lima.data.predicate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import lima.data.dataset.Column;
import lima.data.dataset.Dataset;
import lima.data.domain.DomainSet;
import lima.data.domain.DomainSubSet;

/**
 * Predicate group for categoric (unordered) columns: whether both rows of a
 * pair have the same value in `column`.
 *
 * preds 0 -> pairs with the same value, preds 1 -> pairs with different
 * values (defaults to computing both).
 */
public final class UnorderedPG extends PredicateGroup {
    private final Column values;

    public UnorderedPG(Dataset dataset, String column) {
        super(dataset, column);
        this.values = dataset.getColumn(column);
    }

    @Override
    public int numPredicates() {
        return 2;
    }

    @Override
    public List<byte[]> pgEval(DomainSet<int[]> domain, DomainSubSet x) {
        int[][] pairs = (x == null) ? domain.getFull() : domain.get(x.indices());

        byte[] same = new byte[pairs.length];
        for (int i = 0; i < pairs.length; i++) {
            String left = values.getCategory(pairs[i][0]);
            String right = values.getCategory(pairs[i][1]);
            // raw equality, doesn't care about missing values
            same[i] = (byte) (Objects.equals(left, right) ? 1 : 0);
        }
        return List.of(same);
    }

    @Override
    public Map<Integer, int[]> predPositions(List<byte[]> variables, Set<Integer> preds) {
        Set<Integer> requested = preds != null ? preds : Set.of(0, 1);
        byte[] same = variables.get(0);

        Map<Integer, int[]> result = new LinkedHashMap<>();
        if (requested.contains(0)) {
            result.put(0, where(same, (byte) 1));
        }
        if (requested.contains(1)) {
            result.put(1, where(same, (byte) 0));
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
