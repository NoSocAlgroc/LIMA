package edu.upc.lattice;

public class Pred {

    public int cp,p,score;

    public Pred(int cp, int p, int[] predProd) {
        this.cp=cp;
        this.p=p;
        this.score=(p+1) * predProd[cp];
    }

    @Override
    public int hashCode() {
        return this.score;
    }

    @Override
    public boolean equals(Object obj) {
        Pred other= (Pred)obj;
        return other.cp==this.cp && other.p==this.p;
    }
    
}
