package crystal;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class RealPubsubOutputFactory implements PubsubOutputFactory{
  private final String topicName;

  public RealPubsubOutputFactory(String topicName) {
    this.topicName = topicName;
  }

  @Override
  public PTransform<PCollection<String>, PDone> writer() {
    return PubsubIO.writeStrings().to(topicName);
  }
}
