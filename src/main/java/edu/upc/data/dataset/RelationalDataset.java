package edu.upc.data.dataset;

import edu.upc.data.TPs.TPSubSet;
import edu.upc.data.schema.Predicate;
import edu.upc.data.schema.Schema;

public abstract class RelationalDataset {
    
    public Schema schema;

    public abstract TPSubSet filter(TPSubSet TPs, Predicate pred);

    public abstract TPSubSet sample(int n);

    
}
