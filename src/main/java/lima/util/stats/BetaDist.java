package lima.util.stats;

import java.util.ArrayList;
import java.util.List;

/**
 * Beta(a,b) distribution tracking a selectivity p in (0,1).
 *
 * Sufficient statistics:
 *   meanLO       = E[log(p)]      ~ psi(a) - psi(a+b)
 *   varLO        = Var(log(p))    ~ psi1(a) - psi1(a+b)
 *   meanLogOdds  = E[logit(p)]    ~ psi(a) - psi(b)
 *   varLogOdds   = Var(logit(p))  ~ psi1(a) + psi1(b)
 * Gradients dmda/dmdb and dvda/dvdb are with respect to a and b.
 */
public final class BetaDist {
    public long a;
    public long b;
    public double mean;
    public double meanLO;
    public double varLO;
    public double sdLO;
    public double meanLogOdds;
    public double varLogOdds;
    public double sdLogOdds;
    public double dmda;
    public double dmdb;
    public double dvda;
    public double dvdb;

    private BetaDist(
        long a, long b, double mean, double meanLO, double varLO, double sdLO,
        double meanLogOdds, double varLogOdds, double sdLogOdds,
        double dmda, double dmdb, double dvda, double dvdb
    ) {
        this.a = a;
        this.b = b;
        this.mean = mean;
        this.meanLO = meanLO;
        this.varLO = varLO;
        this.sdLO = sdLO;
        this.meanLogOdds = meanLogOdds;
        this.varLogOdds = varLogOdds;
        this.sdLogOdds = sdLogOdds;
        this.dmda = dmda;
        this.dmdb = dmdb;
        this.dvda = dvda;
        this.dvdb = dvdb;
    }

    /** Uninformative prior Beta(1,1) = Uniform[0,1]. */
    public static BetaDist empty() {
        BetaDist res = new BetaDist(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        res.add(1, 1);
        return res;
    }

    public void add(long a, long b) {
        this.a += a;
        this.b += b;
        updateStats();
    }

    public BetaDist copy() {
        return new BetaDist(
            a, b, mean, meanLO, varLO, sdLO, meanLogOdds, varLogOdds, sdLogOdds,
            dmda, dmdb, dvda, dvdb);
    }

    private void updateStats() {
        mean = (double) a / (a + b);
        meanLO = y1(a) - y1(a + b);
        varLO = y2(a) - y2(a + b);
        sdLO = Math.sqrt(varLO);
        meanLogOdds = y1(a) - y1(b);
        varLogOdds = y2(a) + y2(b);
        sdLogOdds = Math.sqrt(varLogOdds);
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
     * z-score of the interaction between two edges that add the same
     * predicate. from1-&gt;to1 is the primary edge (e.g. (A,B)-&gt;(A,B,C));
     * from2-&gt;to2 is the reference with one fewer parent predicate (e.g.
     * A-&gt;(A,C)). Negative means the primary edge adds more selectivity
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
    // Memoized digamma/trigamma approximations, shared process-wide (pure
    // functions of n).
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
