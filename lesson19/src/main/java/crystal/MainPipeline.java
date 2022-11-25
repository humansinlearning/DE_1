package crystal;

import org.apache.beam.sdk.Pipeline;


public class MainPipeline {
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();
    PipelineComposer pipelineComposer = new PipelineComposer();
    pipelineComposer.compose(pipeline, new RealPubsubInputFactory("p/xxx"), new RealPubsubOutputFactory("topic"));
    pipeline.run();
  }


}
