package crystal.beam;

import crystal.beam.utils.BeamUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineVisitorTest {
  @Test
  public void should_show_pipeline_with_filter_and_2_transforms() {
    Pipeline pipeline = BeamUtils.createPipeline("Filter and 2 transforms");
    List<Integer> numbers = Arrays.asList(1, 100, 200, 201, 202, 203, 330, 400, 500);
    PCollection<Integer> inputNumbers = pipeline.apply("create", Create.of(numbers));
    // every almost native transform is a composite, e.g. filter implements expand method (BTW it's the contract
    // since every PTransform implementation must implement this method)
    PCollection<Integer> filteredNumbers = inputNumbers.apply("filter", Filter.greaterThan(200));
    PCollection<Integer> multipliedNumbers = filteredNumbers
        .apply("map1", MapElements.into(TypeDescriptors.integers()).via(number -> number * 5))
        .apply("map2", MapElements.into(TypeDescriptors.integers()).via(number -> number / 2));
    NodesVisitor visitor = new NodesVisitor();

    pipeline.traverseTopologically(visitor);

    pipeline.run().waitUntilFinish();
    String graph = visitor.stringifyVisitedNodes();
    assertThat(graph).isEqualTo("[ROOT]  -> create[composite](out: create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) -> " +
        "create/Read(CreateSource)[composite](out: create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) ->" +
        " create/Read(CreateSource)/Impulse(out: create/Read(CreateSource)/Impulse.out) ->  " +
        "(in:  create/Read(CreateSource)/Impulse.out) create/Read(CreateSource)/ParDo(OutputSingleSource)[composite](out: create/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource).output) ->  " +
        "(in:  create/Read(CreateSource)/Impulse.out) create/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource)(out: create/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource).output) ->  " +
        "(in:  create/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource).output) create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)[composite](out: create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) ->  " +
        "(in:  create/Read(CreateSource)/ParDo(OutputSingleSource)/ParMultiDo(OutputSingleSource).output) create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper)[composite](out: create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) ->  " +
        "(in:  create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) filter[composite](out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) ->  " +
        "(in:  create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) filter/ParDo(Anonymous)[composite](out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) ->  " +
        "(in:  create/Read(CreateSource)/ParDo(BoundedSourceAsSDFWrapper)/ParMultiDo(BoundedSourceAsSDFWrapper).output) filter/ParDo(Anonymous)/ParMultiDo(Anonymous)(out: filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) ->  " +
        "(in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) map1[composite](out: map1/Map/ParMultiDo(Anonymous).output) ->  (in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) map1/Map[composite](out: map1/Map/ParMultiDo(Anonymous).output) ->  " +
        "(in:  filter/ParDo(Anonymous)/ParMultiDo(Anonymous).output) map1/Map/ParMultiDo(Anonymous)(out: map1/Map/ParMultiDo(Anonymous).output) ->  (in:  map1/Map/ParMultiDo(Anonymous).output) map2[composite](out: map2/Map/ParMultiDo(Anonymous).output) ->  " +
        "(in:  map1/Map/ParMultiDo(Anonymous).output) map2/Map[composite](out: map2/Map/ParMultiDo(Anonymous).output) ->  (in:  map1/Map/ParMultiDo(Anonymous).output) map2/Map/ParMultiDo(Anonymous)(out: map2/Map/ParMultiDo(Anonymous).output)");
  }

}

//** JAVA NIO USES VISITOR PATTERN!!! .. Apache Beam - HERE - visiting pipeline structure

class NodesVisitor implements Pipeline.PipelineVisitor {

  private List<TransformHierarchy.Node> visitedNodes = new ArrayList<>();

  public List<TransformHierarchy.Node> getVisitedNodes() {
    return visitedNodes;
  }

  @Override
  public void enterPipeline(Pipeline p) {
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    visitedNodes.add(node);
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    visitedNodes.add(node);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
  }

  @Override
  public void leavePipeline(Pipeline pipeline) {

  }

  public String stringifyVisitedNodes() {
    return visitedNodes.stream().map(node -> {
      if (node.isRootNode()) {
        return "[ROOT] ";
      }
      String compositeFlagStringified = node.isCompositeNode() ? "[composite]":"";
      String inputs = stringifyValues(node.getInputs());
      String inputsStringified = inputs.isEmpty() ? "":" (in:  " + inputs + ") ";
      String outputs = stringifyValues(node.getOutputs());
      String outputsStringified = outputs.isEmpty() ? "":"(out: " + outputs + ")";
      return inputsStringified + node.getFullName() + compositeFlagStringified + outputsStringified;
    }).collect(Collectors.joining(" -> "));
  }

  private static String stringifyValues(Map<TupleTag<?>, PCollection<?>> values) {
    return values.entrySet().stream()
        .map(entry -> entry.getValue().getName()).collect(Collectors.joining(","));
  }
}
