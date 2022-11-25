package crystal;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class RealPubsubInputFactory implements PubsubInputFactory {
  private final String subscriptionName;

  public RealPubsubInputFactory(String subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  @Override
  public PTransform<PBegin, PCollection<String>> reader() {
    return PubsubIO.readStrings().fromSubscription(subscriptionName);
  }
}
