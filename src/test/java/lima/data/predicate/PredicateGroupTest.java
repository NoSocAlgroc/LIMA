package lima.data.predicate;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import lima.data.dataset.Dataset;

/** Standalone smoke tests for PredicateGroup.buildPredicateGroups - run directly, no test framework wired up yet. */
public final class PredicateGroupTest {

    public static void main(String[] args) throws IOException {
        testBuildPredicateGroupsDispatchesByDtype();
        System.out.println("All PredicateGroup tests passed.");
    }

    private static void testBuildPredicateGroupsDispatchesByDtype() throws IOException {
        Path csv = writeTempCsv(
            "id(Integer),score(Double),label\n" +
            "0,1.5,a\n" +
            "1,1.5,b\n" +
            "2,2.5,a\n" +
            "3,1.5,a\n"
        );
        Dataset dataset = new Dataset(csv.toString(), 4);

        List<PredicateGroup> groups = PredicateGroup.buildPredicateGroups(dataset);

        check(groups.size() == 3, "should build one PredicateGroup per column");

        check(groups.get(0) instanceof OrderedPG, "id(Integer) should build an OrderedPG");
        check(groups.get(0).numPredicates() == 4, "OrderedPG should report 4 predicates");

        check(groups.get(1) instanceof OrderedPG, "score(Double) should build an OrderedPG");
        check(groups.get(1).numPredicates() == 4, "OrderedPG should report 4 predicates");

        check(groups.get(2) instanceof UnorderedPG, "label (category) should build an UnorderedPG");
        check(groups.get(2).numPredicates() == 2, "UnorderedPG should report 2 predicates");

        Files.deleteIfExists(csv);
        System.out.println("testBuildPredicateGroupsDispatchesByDtype OK");
    }

    private static Path writeTempCsv(String content) throws IOException {
        Path path = Files.createTempFile("predicate-group-test", ".csv");
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
