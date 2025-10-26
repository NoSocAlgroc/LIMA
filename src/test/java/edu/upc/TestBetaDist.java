package edu.upc;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.upc.utils.BetaDistribution;

public class TestBetaDist {

    @Test
    public void testGamma() {
        assertEquals(BetaDistribution.y1(999) + 1. / 999, BetaDistribution.y1(1000), 1e-3);
        assertEquals(BetaDistribution.y2(499) + Math.pow(1. / 499, 2), BetaDistribution.y2(500), 1e-3);
        assertEquals(BetaDistribution.y3(199) + Math.pow(1. / 199, 3), BetaDistribution.y3(200), 1e-3);
    }

    @Test
    public void test() {

        BetaDistribution x=new BetaDistribution(1, 10000);
        System.out.println(x.grad);
        x.add(0, 90000);
        System.out.println(x.grad);
    }
}
