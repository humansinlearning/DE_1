package crystal;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public interface PubsubInputFactory extends Serializable {
  PTransform<PBegin, PCollection<String>> reader();
}
