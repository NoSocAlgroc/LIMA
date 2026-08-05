package lima.data.dataset;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps a fixed-size table loaded from a CSV, inferring each column's dtype
 * from a keyword in its name ("Integer" -> INTEGER, "Double" -> DOUBLE,
 * anything else -> CATEGORY). Reads exactly `nrows` data rows; throws if the
 * file has fewer.
 */
public final class Dataset {
    private final List<String> columns;
    private final Map<String, DType> dtypes;
    private final Map<String, Column> data;
    private final int numRows;

    public Dataset(String filePath, int nrows) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                throw new IllegalArgumentException("CSV file is empty (no header row): " + filePath);
            }

            this.columns = parseCsvLine(headerLine);
            this.dtypes = new LinkedHashMap<>();
            DType[] columnDTypes = new DType[columns.size()];
            for (int c = 0; c < columns.size(); c++) {
                DType dtype = inferDType(columns.get(c));
                columnDTypes[c] = dtype;
                dtypes.put(columns.get(c), dtype);
            }

            long[][] longColumns = new long[columns.size()][];
            double[][] doubleColumns = new double[columns.size()][];
            String[][] stringColumns = new String[columns.size()][];
            for (int c = 0; c < columns.size(); c++) {
                switch (columnDTypes[c]) {
                    case INTEGER -> longColumns[c] = new long[nrows];
                    case DOUBLE -> doubleColumns[c] = new double[nrows];
                    case CATEGORY -> stringColumns[c] = new String[nrows];
                }
            }

            int row = 0;
            while (row < nrows) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                if (line.isEmpty()) {
                    continue; // tolerate a trailing blank line
                }

                List<String> fields = parseCsvLine(line);
                for (int c = 0; c < columns.size(); c++) {
                    String field = fields.get(c);
                    String column = columns.get(c);
                    switch (columnDTypes[c]) {
                        case INTEGER -> longColumns[c][row] = parseLongField(field, column, row);
                        case DOUBLE -> doubleColumns[c][row] = parseDoubleField(field, column, row);
                        case CATEGORY -> stringColumns[c][row] = field;
                    }
                }
                row++;
            }

            if (row < nrows) {
                throw new IllegalArgumentException(
                    "requested nrows=" + nrows + " but " + filePath + " only has " + row + " data row(s)");
            }

            this.numRows = nrows;
            this.data = new LinkedHashMap<>();
            for (int c = 0; c < columns.size(); c++) {
                String column = columns.get(c);
                Column built = switch (columnDTypes[c]) {
                    case INTEGER -> Column.ofIntegers(longColumns[c]);
                    case DOUBLE -> Column.ofDoubles(doubleColumns[c]);
                    case CATEGORY -> Column.ofCategories(stringColumns[c]);
                };
                data.put(column, built);
            }
        }
    }

    /** DOUBLE has a native missing-value sentinel (NaN), so a blank field is
     * treated as missing rather than a parse failure. */
    private static double parseDoubleField(String field, String column, int row) {
        if (field.isEmpty()) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(field);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "column '" + column + "', row " + row + ": cannot parse '" + field + "' as a double", e);
        }
    }

    /** long has no missing-value sentinel, so a blank (or otherwise
     * unparseable) field in an INTEGER column is a hard error rather than
     * silently coerced to some magic value. */
    private static long parseLongField(String field, String column, int row) {
        if (field.isEmpty()) {
            throw new IllegalArgumentException(
                "column '" + column + "', row " + row + ": missing value in an INTEGER column is not supported");
        }
        try {
            return Long.parseLong(field);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "column '" + column + "', row " + row + ": cannot parse '" + field + "' as an integer", e);
        }
    }

    public static DType inferDType(String column) {
        if (column.contains("Integer")) {
            return DType.INTEGER;
        }
        if (column.contains("Double")) {
            return DType.DOUBLE;
        }
        return DType.CATEGORY;
    }

    public List<String> getColumns() {
        return columns;
    }

    public DType getDType(String column) {
        return dtypes.get(column);
    }

    public Column getColumn(String column) {
        return data.get(column);
    }

    public int numRows() {
        return numRows;
    }

    /** Minimal RFC4180-style line splitter: handles quoted fields, embedded
     * commas, and "" as an escaped quote. Does not handle quoted newlines. */
    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        field.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    field.append(c);
                }
            } else if (c == '"') {
                inQuotes = true;
            } else if (c == ',') {
                fields.add(field.toString());
                field.setLength(0);
            } else {
                field.append(c);
            }
        }
        fields.add(field.toString());
        return fields;
    }
}
