package crystal;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import java.io.Serializable;

public interface PubsubOutputFactory extends Serializable {
  PTransform<PCollection<String>, PDone> writer();
}
