package lima.data.dataset;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Standalone smoke tests for Dataset - run directly, no test framework wired up yet. */
public final class DatasetTest {

    public static void main(String[] args) throws IOException {
        testDTypeInferenceAndValues();
        testQuotedFieldWithEmbeddedComma();
        testThrowsWhenNotEnoughRows();
        testRealDatasetSmoke();
        testManyRowsAllValuesReadCorrectly();
        testEmptyCategoryFieldIsEmptyString();
        testEmptyDoubleFieldBecomesNaN();
        testEmptyIntegerFieldThrows();
        testMalformedNumericFieldThrowsWithContext();
        testReadsFewerRowsThanFileContains();
        System.out.println("All Dataset tests passed.");
    }

    private static void testDTypeInferenceAndValues() throws IOException {
        Path csv = writeTempCsv(
            "id(Integer),score(Double),label\n" +
            "1,0.5,a\n" +
            "2,1.5,b\n" +
            "3,2.5,c\n"
        );

        Dataset dataset = new Dataset(csv.toString(), 3);

        check(dataset.getColumns().equals(List.of("id(Integer)", "score(Double)", "label")),
            "columns should match header order");
        check(dataset.getDType("id(Integer)") == DType.INTEGER, "id(Integer) should infer INTEGER");
        check(dataset.getDType("score(Double)") == DType.DOUBLE, "score(Double) should infer DOUBLE");
        check(dataset.getDType("label") == DType.CATEGORY, "label should infer CATEGORY");
        check(dataset.numRows() == 3, "numRows should equal nrows requested");

        Column id = dataset.getColumn("id(Integer)");
        check(id.getLong(0) == 1 && id.getLong(1) == 2 && id.getLong(2) == 3, "id values should parse as longs");

        Column score = dataset.getColumn("score(Double)");
        check(score.getDouble(0) == 0.5 && score.getDouble(2) == 2.5, "score values should parse as doubles");

        Column label = dataset.getColumn("label");
        check(label.getCategory(1).equals("b"), "label values should be kept as strings");

        Files.deleteIfExists(csv);
        System.out.println("testDTypeInferenceAndValues OK");
    }

    private static void testQuotedFieldWithEmbeddedComma() throws IOException {
        Path csv = writeTempCsv(
            "city,population(Integer)\n" +
            "\"Honolulu, HI\",350000\n"
        );

        Dataset dataset = new Dataset(csv.toString(), 1);
        check(dataset.getColumn("city").getCategory(0).equals("Honolulu, HI"),
            "quoted field with embedded comma should parse as one field");
        check(dataset.getColumn("population(Integer)").getLong(0) == 350000,
            "field after the quoted one should still parse correctly");

        Files.deleteIfExists(csv);
        System.out.println("testQuotedFieldWithEmbeddedComma OK");
    }

    private static void testThrowsWhenNotEnoughRows() throws IOException {
        Path csv = writeTempCsv(
            "a,b\n" +
            "1,2\n" +
            "3,4\n"
        );

        boolean threw = false;
        try {
            new Dataset(csv.toString(), 5);
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        check(threw, "requesting more rows than the file has should throw IllegalArgumentException");

        Files.deleteIfExists(csv);
        System.out.println("testThrowsWhenNotEnoughRows OK");
    }

    private static void testRealDatasetSmoke() throws IOException {
        String path = "flights.csv";
        if (!Files.exists(Path.of(path))) {
            System.out.println("testRealDatasetSmoke SKIPPED (flights.csv not found in working directory)");
            return;
        }

        Dataset dataset = new Dataset(path, 10);
        check(dataset.numRows() == 10, "numRows should equal requested nrows");
        check(dataset.getDType("Year(Integer)") == DType.INTEGER, "Year(Integer) should infer INTEGER");
        check(dataset.getDType("OriginCityName(String)") == DType.CATEGORY, "...(String) columns should infer CATEGORY");
        check(dataset.getColumn("Year(Integer)").getLong(0) == 2020, "first row's Year(Integer) should be 2020");

        System.out.println("testRealDatasetSmoke OK");
    }

    private static void testManyRowsAllValuesReadCorrectly() throws IOException {
        int n = 500;
        StringBuilder content = new StringBuilder("id(Integer),value(Double),tag\n");
        for (int i = 0; i < n; i++) {
            content.append(i).append(',').append(i * 0.5).append(',').append("tag_").append(i % 7).append('\n');
        }
        Path csv = writeTempCsv(content.toString());

        Dataset dataset = new Dataset(csv.toString(), n);
        check(dataset.numRows() == n, "numRows should equal the full row count");

        Column id = dataset.getColumn("id(Integer)");
        Column value = dataset.getColumn("value(Double)");
        Column tag = dataset.getColumn("tag");
        for (int i = 0; i < n; i++) {
            check(id.getLong(i) == i, "row " + i + ": id should be " + i);
            check(value.getDouble(i) == i * 0.5, "row " + i + ": value should be " + (i * 0.5));
            check(tag.getCategory(i).equals("tag_" + (i % 7)), "row " + i + ": tag should be tag_" + (i % 7));
        }

        Files.deleteIfExists(csv);
        System.out.println("testManyRowsAllValuesReadCorrectly OK");
    }

    private static void testEmptyCategoryFieldIsEmptyString() throws IOException {
        // a second column keeps the row from being an entirely blank line,
        // which the reader intentionally skips (matching pandas' default
        // skip_blank_lines behavior)
        Path csv = writeTempCsv(
            "label,other\n" +
            ",x\n" +
            "b,y\n"
        );

        Dataset dataset = new Dataset(csv.toString(), 2);
        check(dataset.getColumn("label").getCategory(0).equals(""),
            "a blank field in a CATEGORY column should be read as an empty string");
        check(dataset.getColumn("label").getCategory(1).equals("b"),
            "the following row should still parse normally");

        Files.deleteIfExists(csv);
        System.out.println("testEmptyCategoryFieldIsEmptyString OK");
    }

    private static void testEmptyDoubleFieldBecomesNaN() throws IOException {
        // a second column keeps the row from being an entirely blank line,
        // which the reader intentionally skips (matching pandas' default
        // skip_blank_lines behavior)
        Path csv = writeTempCsv(
            "score(Double),other\n" +
            ",x\n" +
            "1.5,y\n"
        );

        Dataset dataset = new Dataset(csv.toString(), 2);
        check(Double.isNaN(dataset.getColumn("score(Double)").getDouble(0)),
            "a blank field in a DOUBLE column should be read as NaN");
        check(dataset.getColumn("score(Double)").getDouble(1) == 1.5,
            "the following row should still parse normally");

        Files.deleteIfExists(csv);
        System.out.println("testEmptyDoubleFieldBecomesNaN OK");
    }

    private static void testEmptyIntegerFieldThrows() throws IOException {
        Path csv = writeTempCsv(
            "count(Integer),other\n" +
            ",x\n"
        );

        String message = null;
        try {
            new Dataset(csv.toString(), 1);
        } catch (IllegalArgumentException e) {
            message = e.getMessage();
        }
        check(message != null, "a blank field in an INTEGER column should throw, since long has no missing-value sentinel");
        check(message.contains("count(Integer)"), "the error message should name the offending column, got: " + message);

        Files.deleteIfExists(csv);
        System.out.println("testEmptyIntegerFieldThrows OK");
    }

    private static void testMalformedNumericFieldThrowsWithContext() throws IOException {
        Path csv = writeTempCsv(
            "count(Integer)\n" +
            "not-a-number\n"
        );

        String message = null;
        try {
            new Dataset(csv.toString(), 1);
        } catch (IllegalArgumentException e) {
            message = e.getMessage();
        }
        check(message != null, "an unparseable field should throw IllegalArgumentException");
        check(message.contains("count(Integer)") && message.contains("row 0"),
            "the error message should point at the offending column and row, got: " + message);

        Files.deleteIfExists(csv);
        System.out.println("testMalformedNumericFieldThrowsWithContext OK");
    }

    private static void testReadsFewerRowsThanFileContains() throws IOException {
        Path csv = writeTempCsv(
            "a,b\n" +
            "1,2\n" +
            "3,4\n" +
            "5,6\n"
        );

        Dataset dataset = new Dataset(csv.toString(), 2);
        check(dataset.numRows() == 2, "should read only the requested number of rows");
        check(dataset.getColumn("a").getCategory(1).equals("3"),
            "should stop after the requested rows, not read the rest of the file");

        Files.deleteIfExists(csv);
        System.out.println("testReadsFewerRowsThanFileContains OK");
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("dataset-test", ".csv");
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(content);
        }
        return path;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("FAILED: " + message);
        }
    }
}
