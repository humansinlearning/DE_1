package crystal.beam;

import crystal.beam.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class ParDoTest implements Serializable  {
  @Test
  public void should_show_no_global_state_existence() {
    Pipeline pipeline = BeamUtils.createPipeline("Global state not existence test");
    PCollection<Integer> numbers123 = pipeline.apply(Create.of(Arrays.asList(1, 2, 3)));
    class IdentityFunction extends DoFn<Integer, Integer> {
      private final List<Integer> numbers;

      public IdentityFunction(List<Integer> numbers) {
        this.numbers = numbers;
      }

      @DoFn.ProcessElement
      public void processElement(ProcessContext processContext) {
        processContext.output(processContext.element());
        numbers.add(processContext.element());
      }

      @DoFn.Teardown
      public void teardown() {
        if (numbers.isEmpty()) {
          throw new IllegalStateException("Numbers on worker should not be empty");
        }
        System.out.println("numbers="+numbers);
      }

    }
    List<Integer> numbers = new ArrayList<>();

    PCollection<Integer> numbersFromChars = numbers123.apply(ParDo.of(new IdentityFunction(numbers)));

    PAssert.that(numbersFromChars).containsInAnyOrder(1, 2, 3);
    assertThat(numbers).isEmpty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void should_show_lifecycle() {
    int workerCount = 1;
    Pipeline pipeline = BeamUtils.createPipeline("Lifecycle test", workerCount);
    PCollection<Integer> numbers1To100 =
        pipeline.apply(Create.of(IntStream.rangeClosed(1, 1_000).boxed().collect(Collectors.toList())));
    class LifecycleHandler extends DoFn<Integer, Integer> {

      @DoFn.Setup
      public void setup() {
        LifecycleEventsHandler.INSTANCE.addSetup();
      }

      @DoFn.StartBundle
      public void initMap(DoFn<Integer, Integer>.StartBundleContext startBundleContext) {
        LifecycleEventsHandler.INSTANCE.addStartBundle();
      }

      @DoFn.ProcessElement
      public void processElement(ProcessContext processContext) {
        processContext.output(processContext.element());
        LifecycleEventsHandler.INSTANCE.addProcessing();
      }

      @DoFn.FinishBundle
      public void finishBundle(DoFn<Integer, Integer>.FinishBundleContext finishBundleContext) {
        LifecycleEventsHandler.INSTANCE.addFinishBundle();
      }

      @DoFn.Teardown
      public void turnOff(){
        LifecycleEventsHandler.INSTANCE.addTeardown();
      }

    }

    PCollection<Integer> processedNumbers = numbers1To100.apply(ParDo.of(new LifecycleHandler()));

    pipeline.run().waitUntilFinish();
    assertThat(LifecycleEventsHandler.INSTANCE.getSetupCount()).isEqualTo(workerCount);
    // The number of bundles is not fixed over the time but it'd vary between 2 and 4
    // It proves however that the @StartBundle method doesn't means the same as @Setup one
    assertThat(LifecycleEventsHandler.INSTANCE.getStartBundleCount()).isGreaterThanOrEqualTo(LifecycleEventsHandler.INSTANCE.getSetupCount());
    assertThat(LifecycleEventsHandler.INSTANCE.getProcessingCount()).isEqualTo(1_000);
    assertThat(LifecycleEventsHandler.INSTANCE.getFinishBundleCount()).isEqualTo(LifecycleEventsHandler.INSTANCE.getStartBundleCount());
    assertThat(LifecycleEventsHandler.INSTANCE.getTeardownCount()).isEqualTo(workerCount);
  }

  enum LifecycleEventsHandler {
    INSTANCE;

    private AtomicInteger setupCalls = new AtomicInteger(0);

    private AtomicInteger startBundleCalls = new AtomicInteger(0);

    private AtomicInteger processingCalls = new AtomicInteger(0);

    private AtomicInteger finishBundleCalls = new AtomicInteger(0);

    private AtomicInteger teardownCalls = new AtomicInteger(0);

    public void addSetup() {
      setupCalls.incrementAndGet();
    }

    public int getSetupCount() {
      return setupCalls.get();
    }

    public void addStartBundle() {
      startBundleCalls.incrementAndGet();
    }

    public int getStartBundleCount() {
      return startBundleCalls.get();
    }

    public void addProcessing() {
      processingCalls.incrementAndGet();
    }

    public int getProcessingCount() {
      return processingCalls.get();
    }

    public void addFinishBundle() {
      finishBundleCalls.incrementAndGet();
    }

    public int getFinishBundleCount() {
      return finishBundleCalls.get();
    }

    public void addTeardown() {
      teardownCalls.incrementAndGet();
    }

    public int getTeardownCount() {
      return teardownCalls.get();
    }
  }

}
