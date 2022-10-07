package crystal.beam;

import crystal.beam.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class PCollectionTest {
  private static final String FILE_1 = "tmp/beam/1";
  private static final String FILE_2 = "tmp/beam/2";

  @BeforeClass
  public static void writeFiles() throws IOException {
    FileUtils.writeStringToFile(new File(FILE_1), "1\n2\n3\n4", "UTF-8");
    FileUtils.writeStringToFile(new File(FILE_2), "5\n6\n7\n8", "UTF-8");
  }

  @AfterClass
  public static void deleteFiles() {
    FileUtils.deleteQuietly(new File(FILE_1));
    FileUtils.deleteQuietly(new File(FILE_2));
  }

  @Test
  public void should_construct_pcollection_from_memory_objects() {
    List<String> letters = Arrays.asList("a", "b", "x", "c", "d", "a");
    Pipeline pipeline = BeamUtils.createPipeline("Creating PCollection from memory");
    PTransform<PBegin, PCollection<String>> input = Create.of(letters);//.withCoder(StringUtf8Coder.of());
    PCollection<String> lettersCollection = PBegin.in(pipeline).apply(input);
    PAssert.that(lettersCollection).containsInAnyOrder("a", "c", "d", "b", "x", "a");

    pipeline.run().waitUntilFinish();


  }

  @Test
  public void should_construct_pcollection_without_applying_transformation_on_it() {
    Pipeline pipeline = BeamUtils.createPipeline("Creating PCollection from file");

    TextIO.Read reader = TextIO.read().from("/tmp/beam/*");
    PCollection<String> readNumbers = pipeline.apply(reader);

    PAssert.that(readNumbers).containsInAnyOrder("1", "2", "3", "4", "5", "6", "7", "8");
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_not_modify_input_pcollection_after_applying_the_transformation() {
    List<String> letters = Arrays.asList("a", "b", "c", "d", "a");
    Pipeline pipeline = BeamUtils.createPipeline("Transforming a PCollection");

    PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
    PCollection<String> aLetters = lettersCollection.apply(Filter.equal("a"));

    PAssert.that(lettersCollection).containsInAnyOrder("a", "b", "c", "d","a");
    PAssert.that(aLetters).containsInAnyOrder("a","a");
    pipeline.run().waitUntilFinish();
  }

  ///
  @Test
  public void should_use_one_pcollection_as_input_for_different_transformations() {
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    Pipeline pipeline = BeamUtils.createPipeline("Using one PCollection as input for different transformations");

    PCollection<String> lettersCollection = pipeline.apply(Create.of(letters));
    PCollection<String> aLetters = lettersCollection.apply(Filter.equal("a"));
    PCollection<String> notALetter = lettersCollection.apply(Filter.greaterThan("b"));

    PAssert.that(lettersCollection).containsInAnyOrder("a", "b", "c", "d");
    PAssert.that(aLetters).containsInAnyOrder("a");
    PAssert.that(notALetter).containsInAnyOrder("c", "d");
    pipeline.run().waitUntilFinish();

    //Stream<String> s1 = Stream.of("a", "b", "c");
    //s1.filter(s -> "a".equals(s)).collect(Collectors.toSet());
    //s1.filter(s-> "b".compareTo(s) < 0).collect(Collectors.toSet());
  }

  @Test
  public void should_get_pcollection_coder() {
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    Pipeline pipeline = BeamUtils.createPipeline("PCollection coder");

    PCollection<String> lettersCollection = pipeline.apply(Create.of(letters)).setCoder(SerializableCoder.of(String.class));
    pipeline.run().waitUntilFinish();

    Coder<String> lettersCoder = lettersCollection.getCoder();
    assertThat(lettersCoder.getClass()).isEqualTo(SerializableCoder.class);
  }

  @Test
  public void should_get_pcollection_metadata() {
    List<String> letters = Arrays.asList("a", "b", "c", "d");
    Pipeline pipeline = BeamUtils.createPipeline("PCollection metadata");

    PCollection<String> lettersCollection = pipeline.apply("A-B-C-D letters", Create.of(letters));
    pipeline.run().waitUntilFinish();

    assertThat(lettersCollection.isBounded()).isEqualTo(PCollection.IsBounded.BOUNDED);
    WindowingStrategy<?, ?> windowingStrategy = lettersCollection.getWindowingStrategy();
    assertThat(windowingStrategy.getWindowFn().getClass()).isEqualTo(GlobalWindows.class);
    assertThat(lettersCollection.getName()).startsWith("A-B-C-D letters");
  }

  @Test
  public void should_create_pcollection_list() {
    Pipeline pipeline = BeamUtils.createPipeline("PCollection list");
    PCollection<String> letters1 = pipeline.apply(Create.of(Arrays.asList("a", "b", "c")));
    PCollection<String> letters2 = pipeline.apply(Create.of(Arrays.asList("d", "e", "f")));
    PCollection<String> letters3 = pipeline.apply(Create.of(Arrays.asList("g", "h", "i")));


    PCollectionList<String> allLetters = PCollectionList.of(letters1).and(letters2).and(letters3);
    List<PCollection<String>> lettersCollections = allLetters.getAll();
    // Try Flatten transform - > "a"..."i" !!!! TO YOU - Try Flatten -

    PAssert.that(lettersCollections.get(0)).containsInAnyOrder("a", "b", "c");
    PAssert.that(lettersCollections.get(1)).containsInAnyOrder("d", "e", "f");
    PAssert.that(lettersCollections.get(2)).containsInAnyOrder("g", "h", "i");
    pipeline.run().waitUntilFinish();
  }
}
