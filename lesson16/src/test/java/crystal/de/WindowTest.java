package crystal.de;

import crystal.de.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class WindowTest implements Serializable {

  @Test
  public void should_construct_fixed_time_window() {
    Pipeline pipeline = BeamUtils.createPipeline("Fixed-time window");

    TestStream<String> testStream = TestStream.create(StringUtf8Coder.of()).addElements(
            TimestampedValue.of("a1", new Instant(1)), TimestampedValue.of("a3", new Instant(1)),
            TimestampedValue.of("a2", new Instant(1)), TimestampedValue.of("a4", new Instant(1)),
            TimestampedValue.of("a6", new Instant(1)), TimestampedValue.of("a5", new Instant(1)),
            TimestampedValue.of("b1", new Instant(2)), TimestampedValue.of("c1", new Instant(3)),
            TimestampedValue.of("d1", new Instant(4)), TimestampedValue.of("d2", new Instant(4))
        )
        .advanceWatermarkToInfinity();

    PCollection<String> windowedLetters = pipeline.apply(testStream).apply(Window.into(FixedWindows.of(new Duration(1))));
    windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.FIXED_TIME)));

    pipeline.run().waitUntilFinish();

    Map<Instant, List<String>> itemsPerWindow = WindowHandlers.FIXED_TIME.getItemsPerWindow();
    List<String> itemsInstant1 = itemsPerWindow.get(new Instant(1));
    assertThat(itemsInstant1).hasSize(6).containsOnly("a1", "a3", "a2", "a4", "a6", "a5");
    List<String> itemsInstant2 = itemsPerWindow.get(new Instant(2));
    assertThat(itemsInstant2).hasSize(1).containsOnly("b1");
    List<String> itemsInstant3 = itemsPerWindow.get(new Instant(3));
    assertThat(itemsInstant3).hasSize(1).containsOnly("c1");
    List<String> itemsInstant4 = itemsPerWindow.get(new Instant(4));
    assertThat(itemsInstant4).hasSize(2).containsOnly("d1", "d2");
  }

  @Test
  public void should_construct_sliding_time_window_with_duplicated_items() {
    Pipeline pipeline = BeamUtils.createPipeline("Sliding-time window with duplicated items");
    TestStream<String> testStream = TestStream.create(StringUtf8Coder.of()).addElements(
        TimestampedValue.of("a1", new Instant(1)), TimestampedValue.of("a2", new Instant(1)),
        TimestampedValue.of("a3", new Instant(1)), TimestampedValue.of("a4", new Instant(1)),
        TimestampedValue.of("b1", new Instant(2)), TimestampedValue.of("c1", new Instant(3)),
        TimestampedValue.of("d1", new Instant(4)), TimestampedValue.of("d2", new Instant(4)))
        .advanceWatermarkToInfinity();


    PCollection<String> windowedLetters = pipeline.apply(testStream)
        .apply(Window.into(SlidingWindows.of(new Duration(2)).every(new Duration(1))));
    windowedLetters.apply(ParDo.of(new DataPerWindowHandler(WindowHandlers.SLIDING_TIME_DUPLICATED)));

    pipeline.run().waitUntilFinish();

    Map<Instant, List<String>> itemsPerWindow = WindowHandlers.SLIDING_TIME_DUPLICATED.getItemsPerWindow();
    List<String> itemsInstant0 = itemsPerWindow.get(new Instant(0));
    assertThat(itemsInstant0).hasSize(4).containsOnly("a1", "a2", "a3", "a4");
    List<String> itemsInstant1 = itemsPerWindow.get(new Instant(1));
    assertThat(itemsInstant1).hasSize(5).containsOnly("a1", "a2", "a3", "a4", "b1");
    List<String> itemsInstant2 = itemsPerWindow.get(new Instant(2));
    assertThat(itemsInstant2).hasSize(2).containsOnly("b1", "c1");
    List<String> itemsInstant3 = itemsPerWindow.get(new Instant(3));
    assertThat(itemsInstant3).hasSize(3).containsOnly("c1", "d1", "d2");
    List<String> itemsInstant4 = itemsPerWindow.get(new Instant(4));
    assertThat(itemsInstant4).hasSize(2).containsOnly("d1", "d2");
  }


  @Test
  public void should_construct_session_window_for_key_value_elements() {
    Pipeline pipeline = BeamUtils.createPipeline("Session window example for key-value elements");
    PCollection<KV<String, String>> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
        TimestampedValue.of(KV.of("a", "a1"), new Instant(1)), TimestampedValue.of(KV.of("a", "a2"), new Instant(1)),
        TimestampedValue.of(KV.of("a", "a3"), new Instant(5)), TimestampedValue.of(KV.of("b", "b1"), new Instant(2)),
        TimestampedValue.of(KV.of("c", "c1"), new Instant(3)), TimestampedValue.of(KV.of("d", "d1"), new Instant(4)),
        TimestampedValue.of(KV.of("d", "d2"), new Instant(4)), TimestampedValue.of(KV.of("a", "a4"), new Instant(5))
    )));

    PCollection<KV<String, String>> windowedLetters = timestampedLetters
        .apply(Window.into(Sessions.withGapDuration(new Duration(2))));

    IntervalWindow window1 = new IntervalWindow(new Instant(1), new Instant(3));
    PAssert.that(windowedLetters).inWindow(window1).containsInAnyOrder(KV.of("a", "a1"), KV.of("a", "a2"));
    IntervalWindow window2 = new IntervalWindow(new Instant(2), new Instant(4));
    PAssert.that(windowedLetters).inWindow(window2).containsInAnyOrder(KV.of("b", "b1"));
    IntervalWindow window3 = new IntervalWindow(new Instant(3), new Instant(5));
    PAssert.that(windowedLetters).inWindow(window3).containsInAnyOrder(KV.of("c", "c1"));
    IntervalWindow window4 = new IntervalWindow(new Instant(4), new Instant(6));
    PAssert.that(windowedLetters).inWindow(window4).containsInAnyOrder(KV.of("d", "d1"), KV.of("d", "d2"));
    // As you can see, the session is key-based. Normally the [a3, a4] fits into window4. However, as their
    // window is constructed accordingly to "a" session, i.e. 1-3, 4 (no data), 5 - 7, they go to the window 5 - 7
    // and not 4 - 6
    IntervalWindow window5 = new IntervalWindow(new Instant(5), new Instant(7));
    PAssert.that(windowedLetters).inWindow(window5).containsInAnyOrder(KV.of("a", "a3"), KV.of("a", "a4"));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_construct_global_window_for_key_value_elements() {
    Pipeline pipeline = BeamUtils.createPipeline("Global window example for key-value elements");
    PCollection<KV<String, String>> timestampedLetters = pipeline.apply(Create.timestamped(Arrays.asList(
        TimestampedValue.of(KV.of("a", "a1"), new Instant(1)), TimestampedValue.of(KV.of("a", "a2"), new Instant(1)),
        TimestampedValue.of(KV.of("a", "a3"), new Instant(5)), TimestampedValue.of(KV.of("b", "b1"), new Instant(2)),
        TimestampedValue.of(KV.of("c", "c1"), new Instant(3)), TimestampedValue.of(KV.of("d", "d1"), new Instant(4)),
        TimestampedValue.of(KV.of("d", "d2"), new Instant(4)), TimestampedValue.of(KV.of("a", "a4"), new Instant(5))
    )));

    PCollection<KV<String, String>> windowedLetters = timestampedLetters
        .apply(Window.into(new GlobalWindows()));

    PAssert.that(windowedLetters).inWindow(GlobalWindow.INSTANCE).containsInAnyOrder(KV.of("a", "a1"),
        KV.of("a", "a2"), KV.of("a", "a3"), KV.of("a", "a4"), KV.of("b", "b1"), KV.of("c", "c1"),
        KV.of("d", "d1"), KV.of("d", "d2"));
    pipeline.run().waitUntilFinish();
  }
}

class DataPerWindowHandler extends DoFn<String, String> {

  private WindowHandlers windowHandler;

  public DataPerWindowHandler(WindowHandlers windowHandler) {
    this.windowHandler = windowHandler;
  }

  @ProcessElement
  public void processElement(ProcessContext processContext, BoundedWindow window) throws InterruptedException {
    IntervalWindow intervalWindow = (IntervalWindow) window;
    windowHandler.addItem(processContext.element(), intervalWindow.start());
    // Add a small sleep - otherwise test results are not deterministic
    Thread.sleep(300);
    processContext.output(processContext.element());
  }

}

enum WindowHandlers {

  FIXED_TIME, SLIDING_TIME_DUPLICATED, SLIDING_TIME_MISSING, CALENDAR;

  private Map<Instant, List<String>> itemsPerWindow = new ConcurrentHashMap<Instant, List<String>>();

  public void addItem(String item, Instant windowInterval) {
    List<String> windowItems = itemsPerWindow.getOrDefault(windowInterval, new ArrayList<>());
    windowItems.add(item);
    itemsPerWindow.put(windowInterval, windowItems);
  }

  public Map<Instant, List<String>> getItemsPerWindow() {
    return itemsPerWindow;
  }

}