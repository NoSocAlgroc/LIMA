package lima.data.domain;

import java.util.Random;

import lima.util.random.RandZ2;

/** Standalone smoke tests for TPDifferenceDomain - run directly, no test framework wired up yet. */
public final class TPDifferenceDomainTest {

    public static void main(String[] args) {
        testAllPairsOutsideOldRegionAndInNewRange();
        testMakeDomainSetSizeMatchesRequested();
        testRegionProportionsMatchWeights();
        testDegenerateSingleRowExpansionHasNoRegionC();
        testThrowsWhenNewRangeNotGreaterThanOldRange();
        testDomainInterfacePolymorphism();
        System.out.println("All TPDifferenceDomain tests passed.");
    }

    private static void testAllPairsOutsideOldRegionAndInNewRange() {
        int oldRange = 30;
        int newRange = 100;
        TPDifferenceDomain domain = new TPDifferenceDomain(oldRange, newRange, new RandZ2(64, new Random(1)));
        TPDomainSet set = domain.makeDomainSet(5000);

        for (int[] pair : set.getFull()) {
            int i = pair[0];
            int j = pair[1];
            check(i >= 0 && i < newRange, "i should be in [0, newRange)");
            check(j >= 0 && j < newRange, "j should be in [0, newRange)");
            check(i != j, "a tuple pair should never repeat the same row index");
            check(!(i < oldRange && j < oldRange), "no pair should land entirely inside the old region [0," + oldRange + ")^2");
        }
        System.out.println("testAllPairsOutsideOldRegionAndInNewRange OK");
    }

    private static void testMakeDomainSetSizeMatchesRequested() {
        TPDifferenceDomain domain = new TPDifferenceDomain(10, 37, new RandZ2(64, new Random(2)));
        for (int size : new int[] {0, 1, 7, 100, 1001}) {
            check(domain.makeDomainSet(size).size() == size,
                "requesting " + size + " pairs should return exactly that many, remainder rounding included");
        }
        System.out.println("testMakeDomainSetSizeMatchesRequested OK");
    }

    private static void testRegionProportionsMatchWeights() {
        int oldRange = 40;
        int newRange = 100;
        int newRowCount = newRange - oldRange;
        long weightA = (long) oldRange * newRowCount;
        long weightB = (long) newRowCount * oldRange;
        long weightC = (long) newRowCount * (newRowCount - 1);
        long total = weightA + weightB + weightC;

        int n = 200_000;
        TPDifferenceDomain domain = new TPDifferenceDomain(oldRange, newRange, new RandZ2(64, new Random(42)));
        TPDomainSet set = domain.makeDomainSet(n);

        long countA = 0, countB = 0, countC = 0;
        for (int[] pair : set.getFull()) {
            boolean iOld = pair[0] < oldRange;
            boolean jOld = pair[1] < oldRange;
            if (iOld && !jOld) {
                countA++;
            } else if (!iOld && jOld) {
                countB++;
            } else if (!iOld) { // both new
                countC++;
            } else {
                throw new AssertionError("FAILED: a pair with both indices old slipped through");
            }
        }

        double expectedA = n * weightA / (double) total;
        double expectedB = n * weightB / (double) total;
        double expectedC = n * weightC / (double) total;

        // generous tolerance - this is a statistical check, not an exact one
        check(closeEnough(countA, expectedA, 0.1), "region A count " + countA + " should be near expected " + expectedA);
        check(closeEnough(countB, expectedB, 0.1), "region B count " + countB + " should be near expected " + expectedB);
        check(closeEnough(countC, expectedC, 0.1), "region C count " + countC + " should be near expected " + expectedC);
        System.out.println("testRegionProportionsMatchWeights OK");
    }

    private static void testDegenerateSingleRowExpansionHasNoRegionC() {
        int oldRange = 20;
        int newRange = 21; // exactly one new row -> no distinct new-new pair possible
        TPDifferenceDomain domain = new TPDifferenceDomain(oldRange, newRange, new RandZ2(64, new Random(5)));
        TPDomainSet set = domain.makeDomainSet(1000);

        for (int[] pair : set.getFull()) {
            boolean bothNew = pair[0] >= oldRange && pair[1] >= oldRange;
            check(!bothNew, "with only one new row, no pair should have both indices in the new region");
        }
        System.out.println("testDegenerateSingleRowExpansionHasNoRegionC OK");
    }

    private static void testThrowsWhenNewRangeNotGreaterThanOldRange() {
        boolean threwEqual = false;
        try {
            new TPDifferenceDomain(10, 10);
        } catch (IllegalArgumentException e) {
            threwEqual = true;
        }
        check(threwEqual, "newRange == oldRange should be rejected");

        boolean threwSmaller = false;
        try {
            new TPDifferenceDomain(10, 5);
        } catch (IllegalArgumentException e) {
            threwSmaller = true;
        }
        check(threwSmaller, "newRange < oldRange should be rejected");
        System.out.println("testThrowsWhenNewRangeNotGreaterThanOldRange OK");
    }

    private static void testDomainInterfacePolymorphism() {
        Domain<TPDomainSet> domain = new TPDifferenceDomain(5, 20, new RandZ2(64, new Random(3)));
        DomainSet<int[]> set = domain.makeDomainSet(50);
        check(set.size() == 50, "polymorphic access through Domain/DomainSet should behave the same");
        System.out.println("testDomainInterfacePolymorphism OK");
    }

    private static boolean closeEnough(double actual, double expected, double relativeTolerance) {
        return Math.abs(actual - expected) <= relativeTolerance * expected;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
