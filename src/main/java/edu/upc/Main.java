package edu.upc;



import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.roaringbitmap.RoaringBitmap;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.dataset.CSVDataset;
import edu.upc.data.schema.Predicate;
import edu.upc.lattice.SchemaLattice;
import edu.upc.lima.Scheduler;
import edu.upc.utils.BetaDistribution;

public class Main {
    public static void main(String[] args) throws Exception{


        System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));


        CSVDataset dataset=new CSVDataset("flights.csv", 1000);

        Scheduler scheduler=new Scheduler(dataset);

        Scheduler.SchedulerLattice lattice=scheduler.schedulerLattice;
        

        Scheduler.SchedulerLattice.Node node=lattice.fetchRoot();

        ArrayList<Scheduler.SchedulerLattice.Node> nodes=new ArrayList<>();
        lattice.fetchSupersets(node, n -> { 
            nodes.add(n);
            //System.out.println(n);
            if(n.preds.size()<2){
                //System.out.println(n);
                lattice.fetchNode(n.preds);
                return false;
            }
            else return true;
        } );       

        ArrayList<Scheduler.SchedulerLattice.Node> currentNodes=nodes;
        double minGrad=1e-6;
        for(int batch=0;batch<15;batch++) {
            System.out.println("Nodes: "+Integer.toString(currentNodes.size()));
            ArrayList<Scheduler.SchedulerLattice.Node> newNodes=new ArrayList<>(currentNodes.size());
            for(Scheduler.SchedulerLattice.Node n:currentNodes) {
                double grad=scheduler.sample(n, batch);
                if(grad>=minGrad) {
                    newNodes.add(n);
                    //System.out.println(grad);
                }
            }

            currentNodes=newNodes;
        }



        System.out.println("Done");

        System.out.flush();
        
    }
}
