package edu.upc.data.schema;

public class ColumnPair {
    public Column x,y;
    public Predicate[] preds;
    public Column.Type type;

    public ColumnPair(Column x, Column y) {
        this.x=x;
        this.y=y;
        this.type=this.x.type;
    }
    

    @Override
    public String toString() {
        return this.x.name+" "+this.y.name;
    }
    
}
