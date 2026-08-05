package lima.util.stats;

/** Standalone smoke tests for BetaDist - run directly, no test framework wired up yet. */
public final class BetaDistTest {

    public static void main(String[] args) {
        testEmptyIsUniform();
        testAddAccumulates();
        testCopyIsIndependent();
        testMoreEvidenceShrinksVariance();
        testSkewTowardsAIncreasesMean();
        testNodeDist4IsZeroForIdenticalEdges();
        testNodeDist4NegativeWhenFirstEdgeIsMoreSelective();
        testLargeNUsesAsymptoticTailWithoutCrashing();
        testNodeGradKnownValue();
        testNodeGradShrinksWithMoreEvidence();
        System.out.println("All BetaDist tests passed.");
    }

    private static void testEmptyIsUniform() {
        BetaDist d = BetaDist.empty();
        check(d.a == 1 && d.b == 1, "empty() should be Beta(1,1)");
        check(d.mean == 0.5, "Beta(1,1)'s mean should be 0.5");
        System.out.println("testEmptyIsUniform OK");
    }

    private static void testAddAccumulates() {
        BetaDist d = BetaDist.empty();
        d.add(3, 7);
        check(d.a == 4 && d.b == 8, "add should accumulate onto the existing counts");
        check(d.mean == 4.0 / 12.0, "mean should update to reflect the new counts");
        System.out.println("testAddAccumulates OK");
    }

    private static void testCopyIsIndependent() {
        BetaDist original = BetaDist.empty();
        original.add(10, 10);
        BetaDist copy = original.copy();

        original.add(100, 0);

        check(copy.a == 11 && copy.b == 11, "copy should keep the state at the time it was made");
        check(original.a == 111 && original.b == 11, "mutating the original after copying should not affect the copy");
        System.out.println("testCopyIsIndependent OK");
    }

    private static void testMoreEvidenceShrinksVariance() {
        BetaDist small = BetaDist.empty();
        small.add(4, 4); // a=5, b=5

        BetaDist large = BetaDist.empty();
        large.add(4999, 4999); // a=5000, b=5000, same ratio, far more evidence

        check(large.varLO < small.varLO, "more evidence at the same ratio should shrink log-odds variance");
        check(large.sdLO == Math.sqrt(large.varLO), "sdLO should always be sqrt(varLO)");
        System.out.println("testMoreEvidenceShrinksVariance OK");
    }

    private static void testSkewTowardsAIncreasesMean() {
        BetaDist skewed = BetaDist.empty();
        skewed.add(999, 0); // a=1000, b=1
        BetaDist balanced = BetaDist.empty();

        check(skewed.mean > balanced.mean, "far more successes than failures should raise the mean");
        check(skewed.meanLO > balanced.meanLO, "...and raise meanLO too (higher log-odds of success)");
        System.out.println("testSkewTowardsAIncreasesMean OK");
    }

    private static void testNodeDist4IsZeroForIdenticalEdges() {
        BetaDist from = BetaDist.empty();
        from.add(50, 10);
        BetaDist to = BetaDist.empty();
        to.add(5, 50);

        // from1==from2 and to1==to2 (same edge compared against itself) -> the
        // two selectivity-drop terms cancel out exactly, regardless of the
        // actual values.
        double z = BetaDist.nodeDist4(from, to, from.copy(), to.copy());
        check(z == 0.0, "nodeDist4 of an edge against an identical reference edge should be exactly 0");
        System.out.println("testNodeDist4IsZeroForIdenticalEdges OK");
    }

    private static void testNodeDist4NegativeWhenFirstEdgeIsMoreSelective() {
        // from1 -> to1: a steep, well-evidenced drop in selectivity
        BetaDist from1 = BetaDist.empty();
        from1.add(4999, 4999); // ~0.5
        BetaDist to1 = BetaDist.empty();
        to1.add(1, 9997); // ~0.0002, extremely selective

        // from2 -> to2: no real drop, same baseline
        BetaDist from2 = BetaDist.empty();
        from2.add(4999, 4999);
        BetaDist to2 = BetaDist.empty();
        to2.add(4999, 4999);

        double z = BetaDist.nodeDist4(from1, to1, from2, to2);
        check(z < -3.0, "an edge that adds real selectivity vs. one that adds none should score strongly negative");
        System.out.println("testNodeDist4NegativeWhenFirstEdgeIsMoreSelective OK");
    }

    private static void testLargeNUsesAsymptoticTailWithoutCrashing() {
        BetaDist d = BetaDist.empty();
        d.add(20_000, 1); // a+b well past both the y1 (5000) and y2 (1000) asymptotic thresholds
        check(Double.isFinite(d.meanLO), "meanLO should stay finite past the asymptotic thresholds");
        check(Double.isFinite(d.varLO) && d.varLO > 0, "varLO should stay finite and positive past the thresholds");
        System.out.println("testLargeNUsesAsymptoticTailWithoutCrashing OK");
    }

    private static void testNodeGradKnownValue() {
        BetaDist d = BetaDist.empty(); // a=1, b=1
        double expected = 1.0 / (1.0 * (1 + 1) * (1 + 1)); // b / (a * s^2) = 1/(1*4)
        check(BetaDist.nodeGrad(d) == expected, "nodeGrad(Beta(1,1)) should be exactly 0.25");
        System.out.println("testNodeGradKnownValue OK");
    }

    private static void testNodeGradShrinksWithMoreEvidence() {
        BetaDist little = BetaDist.empty();
        little.add(4, 4); // a=5, b=5

        BetaDist lots = BetaDist.empty();
        lots.add(4999, 4999); // a=5000, b=5000, same ratio, far more evidence

        check(BetaDist.nodeGrad(lots) < BetaDist.nodeGrad(little),
            "more evidence at the same ratio should lower the sensitivity to one more observation");
        System.out.println("testNodeGradShrinksWithMoreEvidence OK");
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
