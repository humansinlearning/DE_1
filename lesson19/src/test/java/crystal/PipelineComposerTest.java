package crystal;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.*;

public class PipelineComposerTest implements Serializable {
  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPipelineEnd2End() {
    FakePubsubInputFactory inputFactory = new FakePubsubInputFactory(ImmutableList.of("ala", "bala"));
    FakePubsubOutputFactory outputFactory = new FakePubsubOutputFactory((SerializableFunction<PAssert.IterableAssert<String>, PAssert.IterableAssert<String>>)
        input -> input.satisfies((SerializableFunction<Iterable<String>, Void>) input1 -> {
      Iterator<String> iter = input1.iterator();
      assertTrue(iter.hasNext());
      Set<String> elems = new HashSet<>();
      elems.add(iter.next());
      assertTrue(iter.hasNext());
      elems.add(iter.next());
      assertEquals(new HashSet<String>() {{
        add("ALA");
        add("BALA");
      }}, elems);
      assertFalse(iter.hasNext());
      return null;
    }));
    PipelineComposer composer = new PipelineComposer();
    composer.compose(pipeline, inputFactory, outputFactory);
    pipeline.run().waitUntilFinish();
  }
}