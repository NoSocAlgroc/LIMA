package lima.data.domain;

import java.util.Random;

import lima.util.random.RandZ2;

/** Standalone smoke tests for TPDomain/TPDomainSet - run directly, no test framework wired up yet. */
public final class TPDomainTest {

    public static void main(String[] args) {
        testMakeDomainSetSizeAndRange();
        testGetReturnsCopyNotAliasedToBackingArray();
        testGetFullReturnsSameBackingArrayNoCopy();
        testGetSelectsCorrectRows();
        testDomainInterfacePolymorphism();
        testFullSubsetCoversEveryPosition();
        System.out.println("All TPDomain tests passed.");
    }

    private static void testMakeDomainSetSizeAndRange() {
        int range = 43;
        TPDomain domain = new TPDomain(range, new RandZ2(64, new Random(11)));
        TPDomainSet set = domain.makeDomainSet(300);

        check(set.size() == 300, "domain set size should equal the requested size");
        for (int[] pair : set.getFull()) {
            check(pair[0] >= 0 && pair[0] < range, "pair[0] should be in [0, range)");
            check(pair[1] >= 0 && pair[1] < range, "pair[1] should be in [0, range)");
            check(pair[0] != pair[1], "a tuple pair should never repeat the same row index");
        }
        System.out.println("testMakeDomainSetSizeAndRange OK");
    }

    private static void testGetReturnsCopyNotAliasedToBackingArray() {
        int[][] pairs = {{1, 2}, {3, 4}, {5, 6}};
        TPDomainSet set = new TPDomainSet(pairs);

        int[][] fetched = set.get(new int[] {1});
        fetched[0][0] = -1;

        check(pairs[1][0] == 3, "mutating a get() result should not affect the backing pairs array");
        System.out.println("testGetReturnsCopyNotAliasedToBackingArray OK");
    }

    private static void testGetFullReturnsSameBackingArrayNoCopy() {
        int[][] pairs = {{1, 2}, {3, 4}};
        TPDomainSet set = new TPDomainSet(pairs);
        check(set.getFull() == pairs, "getFull() should return the exact backing array, not a copy");
        System.out.println("testGetFullReturnsSameBackingArrayNoCopy OK");
    }

    private static void testGetSelectsCorrectRows() {
        int[][] pairs = {{10, 11}, {20, 21}, {30, 31}, {40, 41}};
        TPDomainSet set = new TPDomainSet(pairs);

        int[][] selected = set.get(new int[] {2, 0, 3});
        check(selected.length == 3, "get() should return one row per requested index");
        check(selected[0][0] == 30 && selected[0][1] == 31, "get() should preserve row order 0");
        check(selected[1][0] == 10 && selected[1][1] == 11, "get() should preserve row order 1");
        check(selected[2][0] == 40 && selected[2][1] == 41, "get() should preserve row order 2");
        System.out.println("testGetSelectsCorrectRows OK");
    }

    private static void testDomainInterfacePolymorphism() {
        Domain<TPDomainSet> domain = new TPDomain(25, new RandZ2(64, new Random(5)));
        DomainSet<int[]> set = domain.makeDomainSet(40);
        check(set.size() == 40, "polymorphic access through Domain/DomainSet should behave the same");
        System.out.println("testDomainInterfacePolymorphism OK");
    }

    private static void testFullSubsetCoversEveryPosition() {
        TPDomainSet set = new TPDomain(15, new RandZ2(64, new Random(2))).makeDomainSet(50);
        DomainSubSet full = set.full();
        check(full.size() == 50, "full() should cover every position in the domain set");
        for (int i = 0; i < 50; i++) {
            check(contains(full.indices(), i), "full()'s indices should include position " + i);
        }
        System.out.println("testFullSubsetCoversEveryPosition OK");
    }

    private static boolean contains(int[] array, int value) {
        for (int v : array) {
            if (v == value) {
                return true;
            }
        }
        return false;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
