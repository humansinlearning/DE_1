package crystal.beam;

import crystal.beam.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Arrays;

@RunWith(JUnit4.class)
public class EventTimeTest implements Serializable {
  @Test
  public void should_change_event_time_for_collection_elements() {
    Pipeline pipeline = BeamUtils.createPipeline("Event time changed");
    Instant now = new Instant(0);
    Instant sec1Duration = now.plus(Duration.standardSeconds(1));
    Instant sec2Duration = now.plus(Duration.standardSeconds(2));
    PCollection<String> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
        TimestampedValue.of("a", now),
        TimestampedValue.of("a", sec1Duration),
        TimestampedValue.of("a", sec1Duration),
        TimestampedValue.of("a", sec1Duration),
        TimestampedValue.of("b", sec2Duration),
        TimestampedValue.of("a", sec1Duration),
        TimestampedValue.of("a", sec1Duration),
        TimestampedValue.of("a", sec1Duration)
    )));
    Duration windowDuration = Duration.standardSeconds(1);
    Window<String> window = Window.into(FixedWindows.of(windowDuration));
    PCollection<String> countResult = timestampedLetters.apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext processContext) {
            processContext.outputWithTimestamp(processContext.element(), processContext.timestamp().plus(1));
          }
        }))
        .apply(window)
        .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
            .via((String letter) -> KV.of(letter, 1)))
        .apply(Count.perKey())
        .apply(MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> pair) ->
            pair.getKey() + "=" + pair.getValue()));

    PAssert.that(countResult).inFinalPane(new IntervalWindow(now, now.plus(windowDuration))).containsInAnyOrder("a=1");

    IntervalWindow window2 = new IntervalWindow(now.plus(windowDuration), now.plus(windowDuration).plus(windowDuration));
    PAssert.that(countResult).inFinalPane(window2).containsInAnyOrder("a=6");
    IntervalWindow window3 = new IntervalWindow(window2.end(), window2.end().plus(windowDuration));
    PAssert.that(countResult).inFinalPane(window3).containsInAnyOrder("b=1");
    pipeline.run().waitUntilFinish();
  }
}
