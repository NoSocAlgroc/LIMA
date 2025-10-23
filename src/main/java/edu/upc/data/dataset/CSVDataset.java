package edu.upc.data.dataset;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import com.csvreader.CsvReader;
import com.koloboke.collect.map.hash.HashObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;

import edu.upc.data.TPs.TPSet;
import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.schema.Column;
import edu.upc.data.schema.Predicate;
import edu.upc.data.schema.Schema;

public class CSVDataset extends RelationalDataset{

    
    
    BufferedReader reader;
    public Schema schema;
    CsvReader csvReader;
    int size;


    public CSVDataset(String path,int size) throws IOException {
        this.reader= new BufferedReader(new FileReader(path));
        this.csvReader=new CsvReader(this.reader, ',');
        this.csvReader.readHeaders();
        this.schema=new Schema(csvReader.getHeaders());
        this.size=size;

        String[][] data=get(size);
        this.buildColumns(data);
    }

    public String[][] get(int n) throws IOException{
        

        String[][] res=new String[n][];
        for(int i=0;i<n;i++) {
            if(!csvReader.readRecord())return null;
            res[i]=csvReader.getValues();
        }
        return res;

    }


    public int[][] intData;
    public float[][] realData;
    public int[][] stringData;
    HashObjIntMap<String> stringMap=HashObjIntMaps.newUpdatableMap();
    ArrayList<String> invStringMap=new ArrayList<>();

        
    void buildColumns(String[][] data)  throws IOException{

        int n=this.size;


        this.intData=new int[schema.typeColumns.get(Column.Type.INTEGER).length][n];
        this.realData=new float[schema.typeColumns.get(Column.Type.REAL).length][n];
        this.stringData=new int[schema.typeColumns.get(Column.Type.STRING).length][n];


        for(Column col:schema.typeColumns.get(Column.Type.INTEGER))
        {
            int typeID=col.typeID;
            int colID=col.ID;
             for(int i=0;i<n;i++) intData[typeID][i]=Integer.parseInt(data[i][colID]);
        }
        for(Column col:schema.typeColumns.get(Column.Type.REAL))
        {
            int typeID=col.typeID;
            int colID=col.ID;
            for(int i=0;i<n;i++) {
                String val=data[i][colID];
                if(val.length()==0) val="NaN";
                realData[typeID][i]=Float.parseFloat(val);
            }
        }
        int defVal=this.stringMap.defaultValue();
        for(Column col:schema.typeColumns.get(Column.Type.STRING))
        {
            int typeID=col.typeID;
            int colID=col.ID;
            for(int i=0;i<n;i++) {
                String k=data[i][colID];
                int v=this.stringMap.size()+1;
                int prev=this.stringMap.putIfAbsent(k, v);
                if(prev!=defVal){
                    v=prev;
                }                    

                stringData[typeID][i]=v;
            }
        }
            
    }

    @Override
    public TPSubSet filter(TPSubSet TPs, Predicate pred) {
        TPSubSet newTPSubset=new TPSubSet(TPs.source);
        int[] tpx=TPs.source.x,tpy=TPs.source.y;
        int[] oldTPs=TPs.TPs;
        int[] newTPs=newTPSubset.TPs;
        int newLength=0;
        switch (pred.cols.type) {
            case STRING:
                int[] sdx=this.stringData[pred.cols.x.typeID];
                int[] sdy=this.stringData[pred.cols.y.typeID];
                switch (pred.op) {
                    case EQ:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(sdx[tx]==sdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case NE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(sdx[tx]!=sdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    default:
                        assert false;                
                }                
                break;



            case INTEGER:
                int[] idx=this.intData[pred.cols.x.typeID];
                int[] idy=this.intData[pred.cols.y.typeID];
                switch (pred.op) {
                    case EQ:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]==idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case NE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]!=idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case GT:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]>idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case LE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]<=idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case LT:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]<idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case GE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(idx[tx]>=idy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    default:
                        assert false;                
                }                
                break;



            case REAL:
                float[] rdx=this.realData[pred.cols.x.typeID];
                float[] rdy=this.realData[pred.cols.y.typeID];
                switch (pred.op) {
                    case EQ:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]==rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case NE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]!=rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case GT:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]>rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case LE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]<=rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case LT:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]<rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    case GE:                        
                        for(int tp:oldTPs) {
                            int tx=tpx[tp],ty=tpy[tp];
                            if(rdx[tx]>=rdy[ty]) newTPs[newLength++]=tp;
                        }                        
                        break;
                    default:
                        assert false;                
                }                
                break;        
            default:
                assert false;
        }

        newTPSubset.resize(newLength);
        return newTPSubset;
    }

    @Override
    public TPSubSet sample(int n) {

        ThreadLocalRandom random=ThreadLocalRandom.current();
        TPSet TPSet=new TPSet(n);
        for(int i=0;i<n;i++) {
            int x =random.nextInt(n);
            int y=random.nextInt(n-1);
            if(y>=x)y++;
            TPSet.x[i]=x;
            TPSet.y[i]=y;
        }
        TPSubSet TPs=new TPSubSet(TPSet);
        return TPs;
    }

    
    
}
