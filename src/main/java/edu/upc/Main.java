package edu.upc;



import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.roaringbitmap.IntIteratorFlyweight;
import org.roaringbitmap.RoaringBitmap;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;

import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.dataset.CSVDataset;
import edu.upc.data.schema.Predicate;
import edu.upc.lattice.SchemaLattice;
import edu.upc.lima.Scheduler;
import edu.upc.utils.BetaDistribution;

import com.univocity.parsers.csv.CsvParser;;

public class Main {
    public static void main(String[] args) throws Exception{

        //args=new String[]{"flights_large.csv","0.00000001","5000"};
        String data=args[0];
        float aprox=Float.parseFloat(args[1]);
        int nrow=Integer.parseInt(args[2]);
        System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("output.txt"))));

        Scheduler.minGrad=aprox*0.01;
        CSVDataset dataset=new CSVDataset(data, nrow);


        System.err.println("Read");
        Scheduler scheduler=new Scheduler(dataset);
        
        scheduler.populatePredicates();



        System.err.println("Done");

        System.out.flush();
        
    }
}
