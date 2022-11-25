package crystal;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

public class FakePubsubInputFactory implements PubsubInputFactory {
  private final List<String> records;

  public FakePubsubInputFactory(List<String> records) {
    this.records = records;
  }


  @Override
  public PTransform<PBegin, PCollection<String>> reader() {
    return TestStream.create(StringUtf8Coder.of()).addElements(records.get(0), records.subList(1, records.size()).toArray(new String[0]))
        .advanceWatermarkToInfinity();
  }
}
