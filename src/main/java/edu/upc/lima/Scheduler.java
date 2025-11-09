package edu.upc.lima;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import com.koloboke.collect.map.IntIntCursor;
import com.koloboke.collect.map.IntObjCursor;

import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.dataset.RelationalDataset;
import edu.upc.data.schema.Predicate;
import edu.upc.data.schema.Predicate.Operator;
import edu.upc.lattice.Lattice;
import edu.upc.lattice.PredSet;
import edu.upc.lattice.SchemaLattice;
import edu.upc.lattice.Lattice.Edge;
import edu.upc.utils.BetaDistribution;

public class Scheduler {

    public static class SchedulerNode {

        BetaDistribution dist=new BetaDistribution();
        boolean satisfied=false;
        ArrayList<TPSubSet> TPs;

    }

    public static class SchedulerEdge {
        BetaDistribution dist=new BetaDistribution();
        boolean sound=false;
    }
    public class SchedulerLattice extends Lattice<SchedulerNode,SchedulerEdge> {

        public SchedulerLattice(Lattice<?, ?> base) {
            super(base);
        }

    }

    public double minGrad=1e-8;
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

    SchedulerEdge getEdge(SchedulerLattice.Edge edge) {
        if(edge.e==null)
        {
            edge.e=new SchedulerEdge();
        }
        return edge.e;
    }

    int getBatchSize(int batch) {
        return 100*(1<<batch);
    }



    public void populatePredicates() {

        Predicate[] preds=this.dataset.schema.preds;
        SchedulerNode[] predNodes=new SchedulerNode[preds.length];
        ArrayList<Integer> predIDs=new ArrayList<>(preds.length);
        for(int i=0;i<preds.length;i++){
            Predicate pred=preds[i];
            SchedulerLattice.Edge e=this.schedulerLattice.fetchRoot().fetchTo(preds[i].cID,preds[i].pID);
            SchedulerLattice.Node n=e.to;
            SchedulerNode sn=this.getNode(n);
            SchedulerEdge se=this.getEdge(e);

            predNodes[i]=sn;
            sn.TPs=new ArrayList<>();

            se.sound=true;           

            

            if(this.dataset.constColumn[pred.cols.x.ID] && (pred.op==Operator.NE || pred.op==Operator.GT || pred.op==Operator.LT))
            {
                sn.dist.add(0, 1000000000);

            }
            else {
                predIDs.add(i);
            }

            
        }

        int batches=20;
        for(int batch=0;batch<batches;batch++) {
            ArrayList<Integer> nextPredIDs=new ArrayList<>(predIDs.size());

            int batchSize=this.getBatchSize(batch);
            TPSubSet TPs=dataset.sample(batchSize);
            for(int predID:predIDs) {
                Predicate pred=preds[predID];
                SchedulerNode predNode=predNodes[predID];
                SchedulerEdge predEdge=this.getEdge(this.schedulerLattice.fetchRoot().fetchTo(pred.cID, pred.pID));

                TPSubSet predTPs=this.dataset.filter(TPs, pred);
                predNode.TPs.add(predTPs);

                int a=predTPs.length;
                int b=batchSize-a;
                predNode.dist.add(a, b);
                predEdge.dist.add(a, b);

                System.out.println("Fetch: ("+pred.cID+", "+pred.pID+"): "+batchSize+" -> "+a+"      "+predNode.dist.grad);

                if(predNode.dist.grad>minGrad) {
                    nextPredIDs.add(predID);
                }
            }

            predIDs=nextPredIDs;
            if(predIDs.size()==0)break;
        }


        Integer[] sortedPreds=new Integer[preds.length];
        for(int i=0;i<preds.length;i++)sortedPreds[i]=i;
        Arrays.sort(sortedPreds,new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
                return -Double.compare(predNodes[a].dist.meanLogOdds,predNodes[b].dist.meanLogOdds);
            }
            
        });


        ArrayList<PredSet> res=new ArrayList<>();

        for(int newPredSortedID=0;newPredSortedID<preds.length;newPredSortedID++) {
            int newPredIDx=sortedPreds[newPredSortedID];
            Predicate newPred=preds[newPredIDx];
            SchedulerLattice.Node node=this.schedulerLattice.fetchRoot().getTo(newPred.cID, newPred.pID).to;

            this.search(node, newPredSortedID, sortedPreds, preds,res);
        }


        for(int i=0;i<preds.length;i++) {
            System.out.println(i+" "+preds[i].cID+"="+preds[i].pID+"  "+preds[i].toString());
        }


        for(PredSet ps:res) {

            String s="!(";

            for(IntIntCursor cur=ps.cursor();cur.moveNext();) {
                int cp=cur.key();
                int p=cur.value();
                s+=this.dataset.schema.columnPairs[cp].preds[p].toString()+" & ";
            }
            s+=")";
            System.out.println(s);

        }

        System.out.println("Done");

    }


    void search(SchedulerLattice.Node node,int lastPredIdx, Integer[] predIDXs,Predicate[] preds,ArrayList<PredSet> res) {

        for(int newPredSortedID=0;newPredSortedID<lastPredIdx;newPredSortedID++) {
            int newPredIDx=predIDXs[newPredSortedID];
            Predicate newPred=preds[newPredIDx];
            if(node.preds.contains(newPred.cID))continue;

            SchedulerLattice.Edge toE=node.fetchTo(newPred.cID, newPred.pID);
            SchedulerLattice.Node toN=toE.to;

            if(exploreNode(toE))
            {
                this.propagateAcross(toE,res);
                search(toN, newPredSortedID, predIDXs, preds,res);
            }


            

        }
    }


    void propagateAcross(SchedulerLattice.Edge e,ArrayList<PredSet> res) {

        SchedulerNode fn=this.getNode(e.from);
        SchedulerNode tn=this.getNode(e.to);

        SchedulerEdge se=this.getEdge(e);
        

        tn.TPs=new ArrayList<>();
        int a=0,b=0;
        for(TPSubSet TPs:fn.TPs) {
            TPSubSet filteredTPs=this.dataset.filter(TPs, this.dataset.schema.columnPairs[e.cp].preds[e.p]);
            tn.TPs.add(filteredTPs);

            a+=filteredTPs.length;
            b+=TPs.source.length-filteredTPs.length;          
            
        }
        tn.dist.add(a, b);
        se.dist.add(tn.dist.a-1, fn.dist.a-tn.dist.a);

        se.sound=true;

        for(IntObjCursor<SchedulerLattice.Edge> cur=e.from.from.cursor();cur.moveNext();) {
            SchedulerLattice.Node fNode=cur.value().from;
            SchedulerLattice.Edge tEdge=fNode.fetchTo(e.cp, e.p);
            SchedulerLattice.Node tNode=tEdge.to;

            SchedulerEdge tSEdge=this.getEdge(tEdge);

            BetaDistribution lower=tSEdge.dist;
            BetaDistribution upper=se.dist;

            double devs=4;
            double lowerMinLogProb=lower.meanLogOdds-devs*lower.sdLogOdds;
            double upperMaxLogProb=upper.meanLogOdds+devs*upper.sdLogOdds;

            if(upperMaxLogProb+0.2>lowerMinLogProb) 
            {
                se.sound=false;
                break;
            }
            else {
                se.sound=true;
            }

        }

        if(se.sound && se.dist.a==1) {
            res.add(e.to.preds);
        }



        System.out.println("Propagate: "+e.from.toString()+" + ("+e.cp+"="+e.p+"): "+(fn.dist.a-1)+"->"+a+" Sound?:"+se.sound);

    }
    boolean exploreNode(SchedulerLattice.Edge e) {
        if(e.to.preds.size()>6) return false;
        if(this.getNode(e.from).dist.a==1) return false;


        for(IntObjCursor<SchedulerLattice.Edge> cur=e.from.from.cursor();cur.moveNext();) {
            SchedulerLattice.Node fNode=cur.value().from;
            SchedulerLattice.Edge tEdge=fNode.fetchTo(e.cp, e.p);
            SchedulerLattice.Node tNode=tEdge.to;

            SchedulerEdge tSEdge=this.getEdge(tEdge);

            if(!tSEdge.sound) return false;
            

            SchedulerNode tSNode=this.getNode(tNode);
            if(tSNode.dist.a==1) return false;

        }


        return true;
    }





}
