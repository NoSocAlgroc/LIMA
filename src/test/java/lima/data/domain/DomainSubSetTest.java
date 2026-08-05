package lima.data.domain;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/** Standalone smoke tests for DomainSubSet - run directly, no test framework wired up yet. */
public final class DomainSubSetTest {

    public static void main(String[] args) {
        testFullContainsEveryIndex();
        testMakeSparseRoundTrip();
        testMakeDenseRoundTrip();
        testMakeEmptyIndices();
        testAndAgainstReference_denseDense();
        testAndAgainstReference_sparseSparse();
        testAndAgainstReference_mixed();
        testAndRandomizedAgainstReference();
        testFullAndSelfIsFull();
        testDifferentSizeThrows();
        System.out.println("All DomainSubSet tests passed.");
    }

    private static void testFullContainsEveryIndex() {
        int n = 37;
        DomainSubSet full = DomainSubSet.full(n);
        check(full.size() == n, "full(n) should have size n");
        check(Arrays.equals(full.indices(), range(n)), "full(n)'s indices should be 0..n-1");
        System.out.println("testFullContainsEveryIndex OK");
    }

    private static void testMakeSparseRoundTrip() {
        int n = 1000;
        int[] idx = {3, 17, 500, 999}; // well under 10% of n -> sparse representation
        DomainSubSet subset = DomainSubSet.make(n, idx);
        check(subset.size() == idx.length, "size should equal the number of indices");
        check(Arrays.equals(subset.indices(), idx), "indices() should round-trip the input");
        System.out.println("testMakeSparseRoundTrip OK");
    }

    private static void testMakeDenseRoundTrip() {
        int n = 100;
        int[] idx = range(60); // 60% of n -> dense representation
        DomainSubSet subset = DomainSubSet.make(n, idx);
        check(subset.size() == idx.length, "size should equal the number of indices");
        check(Arrays.equals(subset.indices(), idx), "indices() should round-trip the input regardless of internal representation");
        System.out.println("testMakeDenseRoundTrip OK");
    }

    private static void testMakeEmptyIndices() {
        DomainSubSet subset = DomainSubSet.make(50, new int[0]);
        check(subset.size() == 0, "an empty index array should produce an empty subset");
        check(subset.indices().length == 0, "indices() should be empty too");
        System.out.println("testMakeEmptyIndices OK");
    }

    private static void testAndAgainstReference_denseDense() {
        int n = 200;
        int[] a = range(0, 150); // 75% of n -> dense
        int[] b = range(50, 200); // 75% of n -> dense
        checkAndMatchesReference(n, a, b, "denseDense");
        System.out.println("testAndAgainstReference_denseDense OK");
    }

    private static void testAndAgainstReference_sparseSparse() {
        int n = 200;
        int[] a = {1, 5, 10, 50, 100, 150}; // well under 10% -> sparse
        int[] b = {5, 10, 60, 100, 199};    // well under 10% -> sparse
        checkAndMatchesReference(n, a, b, "sparseSparse");
        System.out.println("testAndAgainstReference_sparseSparse OK");
    }

    private static void testAndAgainstReference_mixed() {
        int n = 200;
        int[] denseSide = range(0, 180); // 90% of n -> dense
        int[] sparseSide = {2, 5, 100, 179, 199}; // well under 10% -> sparse
        checkAndMatchesReference(n, denseSide, sparseSide, "mixed");
        System.out.println("testAndAgainstReference_mixed OK");
    }

    private static void testAndRandomizedAgainstReference() {
        Random random = new Random(123);
        int n = 300;
        for (int trial = 0; trial < 50; trial++) {
            int[] a = randomSubset(random, n);
            int[] b = randomSubset(random, n);
            checkAndMatchesReference(n, a, b, "randomized trial " + trial);
        }
        System.out.println("testAndRandomizedAgainstReference OK");
    }

    private static void testFullAndSelfIsFull() {
        int n = 64;
        DomainSubSet result = DomainSubSet.full(n).and(DomainSubSet.full(n));
        check(Arrays.equals(result.indices(), range(n)), "full(n) & full(n) should still be every index");
        System.out.println("testFullAndSelfIsFull OK");
    }

    private static void testDifferentSizeThrows() {
        boolean threw = false;
        try {
            DomainSubSet.full(10).and(DomainSubSet.full(20));
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        check(threw, "and() across DomainSubSets of different n should throw IllegalArgumentException");
        System.out.println("testDifferentSizeThrows OK");
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    private static void checkAndMatchesReference(int n, int[] a, int[] b, String label) {
        DomainSubSet result = DomainSubSet.make(n, a).and(DomainSubSet.make(n, b));

        Set<Integer> reference = new HashSet<>();
        for (int x : a) {
            reference.add(x);
        }
        reference.retainAll(toSet(b));

        int[] expected = new TreeSet<>(reference).stream().mapToInt(Integer::intValue).toArray();
        check(Arrays.equals(result.indices(), expected),
            "and() result should match a brute-force set intersection (" + label + "): "
                + "expected " + Arrays.toString(expected) + " got " + Arrays.toString(result.indices()));
        check(result.size() == expected.length, "size() should match the intersection size (" + label + ")");
    }

    private static Set<Integer> toSet(int[] values) {
        Set<Integer> set = new HashSet<>();
        for (int v : values) {
            set.add(v);
        }
        return set;
    }

    private static int[] randomSubset(Random random, int n) {
        // bias towards small subsets about half the time, to exercise both
        // the sparse and dense DomainSubSet representations
        int maxSize = random.nextBoolean() ? Math.max(1, n / 20) : n;
        int size = random.nextInt(maxSize + 1);

        Set<Integer> chosen = new TreeSet<>();
        while (chosen.size() < size) {
            chosen.add(random.nextInt(n));
        }
        return chosen.stream().mapToInt(Integer::intValue).toArray();
    }

    private static int[] range(int endExclusive) {
        return range(0, endExclusive);
    }

    private static int[] range(int startInclusive, int endExclusive) {
        int[] result = new int[endExclusive - startInclusive];
        for (int i = 0; i < result.length; i++) {
            result[i] = startInclusive + i;
        }
        return result;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
