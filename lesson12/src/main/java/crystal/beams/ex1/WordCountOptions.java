package crystal.beams.ex1;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface WordCountOptions extends PipelineOptions {
  @Description("Path for input file.")
  String getInputFilePattern();

  void setInputFilePattern(String inputFile);

  @Description("Path for output file.")
  String getOutputFilePattern();

  void setOutputFilePattern(String outputFilePattern);
}
