package crystal;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class PipelineComposer implements Serializable {
  public void compose(Pipeline pipeline, PubsubInputFactory inputFactory, PubsubOutputFactory outputFactory) {
    PCollection<String> pcoll1 = pipeline.apply("Read from pubsub", inputFactory.reader());
    PCollection<String> pcoll2 = pcoll1.apply("Mapping",
        MapElements.into(StringUtf8Coder.of().getEncodedTypeDescriptor())
            .via(new SerializableFunction<String, String>() {
              @Override
              public String apply(String input) {
                return input.toUpperCase();
              }
            }));
    pcoll2.apply("Write to pubsub", outputFactory.writer());
  }
}
