package lima.util.stats;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks a Beta(a,b) distribution representing selectivity p in (0,1).
 *
 * Sufficient statistics in log-odds space:
 *   meanLO = E[logit(p)] ~ psi(a) - psi(a+b)
 *   varLO  = Var[logit(p)] ~ psi1(a) - psi1(a+b)
 * Gradients dmda/dmdb and dvda/dvdb are with respect to a and b.
 *
 * Only digamma (psi) and trigamma (psi1) approximations are ported here
 * (as y1/y2 below) - the reference implementation also defines tetragamma/
 * pentagamma approximations (y3/y4) and several other statistics (dist1,
 * dist2, grad2, gradsq, distNode, BBDist), but none of them are used
 * anywhere in the lattice/scheduler/sampler packages, so they're left out
 * for now rather than ported speculatively.
 */
public final class BetaDist {
    public long a;
    public long b;
    public double mean;
    public double meanLO;
    public double varLO;
    public double sdLO;
    public double dmda;
    public double dmdb;
    public double dvda;
    public double dvdb;

    private BetaDist(
        long a, long b, double mean, double meanLO, double varLO, double sdLO,
        double dmda, double dmdb, double dvda, double dvdb
    ) {
        this.a = a;
        this.b = b;
        this.mean = mean;
        this.meanLO = meanLO;
        this.varLO = varLO;
        this.sdLO = sdLO;
        this.dmda = dmda;
        this.dmdb = dmdb;
        this.dvda = dvda;
        this.dvdb = dvdb;
    }

    /** Uninformative prior Beta(1,1) = Uniform[0,1]. */
    public static BetaDist empty() {
        BetaDist res = new BetaDist(0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        res.add(1, 1);
        return res;
    }

    public void add(long a, long b) {
        this.a += a;
        this.b += b;
        updateStats();
    }

    public BetaDist copy() {
        return new BetaDist(a, b, mean, meanLO, varLO, sdLO, dmda, dmdb, dvda, dvdb);
    }

    private void updateStats() {
        mean = (double) a / (a + b);
        meanLO = y1(a) - y1(a + b);
        varLO = y2(a) - y2(a + b);
        sdLO = Math.sqrt(varLO);
        dmda = 1.0 / a;
        dmdb = -1.0 / b;
        dvda = -dmda * dmda;
        dvdb = -dmdb * dmdb;
    }

    @Override
    public String toString() {
        return "(" + a + "," + b + ")";
    }

    /**
     * z-score of the interaction between two edges sharing the same added
     * predicate. from1-&gt;to1 is the original edge (e.g. (A,B)-&gt;(A,B,C));
     * from2-&gt;to2 is a reference edge with one fewer parent predicate (e.g.
     * A-&gt;(A,C)). Negative means the original edge adds more selectivity
     * than the reference.
     */
    public static double nodeDist4(BetaDist from1, BetaDist to1, BetaDist from2, BetaDist to2) {
        double m = (to1.meanLO - from1.meanLO) - (to2.meanLO - from2.meanLO);
        double v = to1.varLO + from1.varLO + to2.varLO + from2.varLO;
        return m / Math.sqrt(v);
    }

    /**
     * Sensitivity of the mean to one additional observation
     * (d(mean)/da = b/(a+b)^2). High gradient means the mean will shift
     * more per sample, i.e. worth scheduling.
     */
    public static double nodeGrad(BetaDist d) {
        double s = d.a + d.b;
        return d.b / (d.a * s * s);
    }

    // ------------------------------------------------------------------
    // Memoized digamma/trigamma approximations, process-wide (pure
    // functions of n, safe to share across every BetaDist instance -
    // mirrors the module-level memoized closures in the Python original).
    // ------------------------------------------------------------------

    private static final double EULER_MASCHERONI = 0.5772156649;
    private static final List<Double> Y1_MEM = new ArrayList<>(List.of(-EULER_MASCHERONI));
    private static final List<Double> Y2_MEM = new ArrayList<>(List.of(Math.PI * Math.PI / 6));

    /** y1(n) ~ digamma(n) = psi(n), y1(1) = -gamma. */
    private static double y1(long n) {
        if (n >= 5000) {
            return Math.log(n);
        }
        while (Y1_MEM.size() < n) {
            int prevLen = Y1_MEM.size();
            Y1_MEM.add(Y1_MEM.get(prevLen - 1) + 1.0 / prevLen);
        }
        return Y1_MEM.get((int) n - 1);
    }

    /** y2(n) ~ trigamma(n) = psi1(n), y2(1) = pi^2/6. */
    private static double y2(long n) {
        if (n >= 1000) {
            return 1.0 / n;
        }
        while (Y2_MEM.size() < n) {
            int prevLen = Y2_MEM.size();
            Y2_MEM.add(Y2_MEM.get(prevLen - 1) - 1.0 / ((double) prevLen * prevLen));
        }
        return Y2_MEM.get((int) n - 1);
    }
}
