package crystal.de.utils;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log {
  private static final Logger LOG = LoggerFactory.getLogger(Log.class);

  private Log() {
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements() {
    return new LoggingTransform<>();
  }

  public static <T> PTransform<PCollection<T>, PCollection<T>> ofElements(String prefix) {
    return new LoggingTransform<>(prefix);
  }

  private static class LoggingTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    private String prefix;

    private LoggingTransform() {
      prefix = "";
    }

    private LoggingTransform(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input.apply(
          ParDo.of(
              new DoFn<T, T>() {

                @ProcessElement
                public void processElement(
                    @Element T element, OutputReceiver<T> out, BoundedWindow window) {

                  String message = prefix + element.toString();

                  if (!(window instanceof GlobalWindow)) {
                    message = message + "  Window:" + window.toString();
                  }

                  LOG.info(message);

                  out.output(element);
                }
              }));
    }
  }
}
