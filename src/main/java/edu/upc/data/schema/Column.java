package edu.upc.data.schema;

public class Column {
    public enum Type {
        STRING, REAL, INTEGER
    }
    public String name;
    public Type type;
    public int ID,typeID;

    public Column(String name) 
    {
        this.name=name;
        if (this.name.contains("String")) {
            this.type=Type.STRING;
        }                
        else if (this.name.contains("Double")) {
            this.type=Type.REAL;
        }
        else if (this.name.contains("Integer")) {
            this.type=Type.INTEGER;
        }
    }


    @Override
    public boolean equals(Object obj) {
        Column other=(Column) obj;
        return other.type==this.type && other.name==this.name;
    }
}
