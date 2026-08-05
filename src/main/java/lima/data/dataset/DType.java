package lima.data.dataset;

/** A column's inferred value type, mirroring the Python port's int64/float64/category split. */
public enum DType {
    INTEGER,
    DOUBLE,
    CATEGORY
}
