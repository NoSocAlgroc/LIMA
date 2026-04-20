package lima.data.dataset;

/** A single dataset column: a fixed-size, homogeneously-typed array of values. */
public final class Column {
    private final DType dtype;
    private final long[] longValues;
    private final double[] doubleValues;
    private final String[] stringValues;

    private Column(DType dtype, long[] longValues, double[] doubleValues, String[] stringValues) {
        this.dtype = dtype;
        this.longValues = longValues;
        this.doubleValues = doubleValues;
        this.stringValues = stringValues;
    }

    static Column ofIntegers(long[] values) {
        return new Column(DType.INTEGER, values, null, null);
    }

    static Column ofDoubles(double[] values) {
        return new Column(DType.DOUBLE, null, values, null);
    }

    static Column ofCategories(String[] values) {
        return new Column(DType.CATEGORY, null, null, values);
    }

    public DType dtype() {
        return dtype;
    }

    public int size() {
        return switch (dtype) {
            case INTEGER -> longValues.length;
            case DOUBLE -> doubleValues.length;
            case CATEGORY -> stringValues.length;
        };
    }

    public long getLong(int row) {
        requireDType(DType.INTEGER);
        return longValues[row];
    }

    public double getDouble(int row) {
        requireDType(DType.DOUBLE);
        return doubleValues[row];
    }

    public String getCategory(int row) {
        requireDType(DType.CATEGORY);
        return stringValues[row];
    }

    private void requireDType(DType expected) {
        if (dtype != expected) {
            throw new IllegalStateException("column has dtype " + dtype + ", not " + expected);
        }
    }
}
