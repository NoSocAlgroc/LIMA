package edu.upc.lima;

import java.util.ArrayList;

import com.koloboke.collect.map.IntObjCursor;

import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.dataset.RelationalDataset;
import edu.upc.lattice.Lattice;
import edu.upc.lattice.Lattice.Edge;
import edu.upc.utils.BetaDistribution;

public class Scheduler {

    public class SchedulerNode {

        ArrayList<TPSubSet> batches=new ArrayList<>();
        BetaDistribution dist=new BetaDistribution();

    }
    public class SchedulerLattice extends Lattice<SchedulerNode,Object> {

        public SchedulerLattice(Lattice<?, ?> base) {
            super(base);
        }

    }


    public RelationalDataset dataset;
    public SchedulerLattice schedulerLattice;
    public Scheduler(RelationalDataset dataset) {
        this.dataset=dataset;
        this.schedulerLattice=new SchedulerLattice(dataset.schema.lattice);
    }


    SchedulerNode getNode(SchedulerLattice.Node node) {
        if(node.n==null)
        {
            node.n=new SchedulerNode();
        }
        return node.n;
    }

    int getBatchSize(int batch) {
        return 100*(1<<batch);
    }

    public TPSubSet get(SchedulerLattice.Node node,int batch) {
        SchedulerNode sn=this.getNode(node);

        while (sn.batches.size()<=batch) {
            int neededBatch=sn.batches.size();
            SchedulerLattice.Edge minEdge=this.getMinProb(node);
            TPSubSet tps=null;
            if(minEdge==null)
            {
                int batchSize=this.getBatchSize(batch);
                tps=this.dataset.sample(batchSize);
            }
            else {
                tps=this.get(minEdge.from,neededBatch);
                int l1=tps.length;
                
                tps=this.dataset.filter(tps, this.dataset.schema.columnPairs[minEdge.cp].preds[minEdge.p]);
                int l2=tps.length;
                System.out.println("Filter: "+l1+" to "+l2);
            }
            sn.batches.add(tps);
        }
        return sn.batches.get(batch);
    }

    public double sample(SchedulerLattice.Node node,int batch) {
        TPSubSet TPs=this.get(node, batch);
        int a=TPs.length;
        int b=TPs.source.length-a;

        SchedulerNode sn=this.getNode(node);
        sn.dist.add(a, b);
        return sn.dist.grad;
    }

    SchedulerLattice.Edge getMinProb(SchedulerLattice.Node node) {

        SchedulerLattice.Edge minEdge = null;
        double minProb = 1;

        for (IntObjCursor<SchedulerLattice.Edge> cur = node.from.cursor(); cur.moveNext();) {
            SchedulerLattice.Edge fromEdge = cur.value();
            double newMinProb = this.getNode(fromEdge.from).dist.mean;
            if (newMinProb < minProb) {
                minEdge = fromEdge;
                minProb = newMinProb;
            }
        }
        return minEdge;
    }

}
