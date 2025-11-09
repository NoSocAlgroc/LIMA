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
    CsvReader csvReader;
    int size;


    public CSVDataset(String path,int size) throws IOException {
        this.reader= new BufferedReader(new FileReader(path));
        this.csvReader=new CsvReader(this.reader, ',');
        this.csvReader.readHeaders();
        this.schema=new Schema(csvReader.getHeaders());
        this.size=size;
        this.buildColumns();
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

        
    void buildColumns()  throws IOException{

        int n=this.size;

        int defVal=this.stringMap.defaultValue();

        this.constColumn=new boolean[this.schema.columns.length];
        for(int i=0;i<constColumn.length;i++)constColumn[i]=true;

        Column[] intCols=schema.typeColumns.get(Column.Type.INTEGER);
        int numIntCols=intCols.length;
        this.intData=new int[numIntCols][n];
        int[] intColID=new int[numIntCols];
        for(int i=0;i<numIntCols;i++) {
            intColID[i]=intCols[i].ID;
            assert intCols[i].ID==i;
        }
        
        Column[] realCols=schema.typeColumns.get(Column.Type.REAL);
        int numRealCols=realCols.length;
        this.realData=new float[numRealCols][n];
        int[] realColID=new int[numRealCols];
        for(int i=0;i<numRealCols;i++) {
            realColID[i]=realCols[i].ID;
            assert realCols[i].ID==i;
        }
        Column[] strCols=schema.typeColumns.get(Column.Type.STRING);
        int numStrCols=strCols.length;
        this.stringData=new int[numStrCols][n];
        int[] strColID=new int[numStrCols];
        for(int i=0;i<numStrCols;i++) {
            strColID[i]=strCols[i].ID;
            assert strCols[i].ID==i;
        }

        for(int row=0;row<n;row++){
            csvReader.readRecord();
            String[] fields=this.csvReader.getValues();

            for(int i=0;i<numIntCols;i++){
                int colID=intColID[i];
                intData[i][row]=Integer.parseInt(fields[colID]);
                if(constColumn[colID]) {
                    if(intData[i][row]!=intData[i][0])constColumn[colID]=false;
                }
            }
            for(int i=0;i<numRealCols;i++){
                int colID=realColID[i];
                realData[i][row]=Float.parseFloat(fields[colID]);
                if(constColumn[colID]) {
                    if(realData[i][row]!=realData[i][0])constColumn[colID]=false;
                }
            }
            for(int i=0;i<numStrCols;i++){
                int colID=strColID[i];
                String k=fields[colID];
                int v=this.stringMap.size()+1;
                int prev=this.stringMap.putIfAbsent(k, v);
                if(prev!=defVal){
                    v=prev;
                } 
                stringData[i][row]=v;
                if(constColumn[colID]) {
                    if(stringData[i][row]!=stringData[i][0])constColumn[colID]=false;
                }
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
            int x =random.nextInt(this.size);
            int y=random.nextInt(this.size-1);
            if(y>=x)y++;
            TPSet.x[i]=x;
            TPSet.y[i]=y;
        }
        TPSubSet TPs=new TPSubSet(TPSet);
        return TPs;
    }

    
    
}
