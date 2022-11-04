package crystal;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface JobOptions extends PipelineOptions {
  @Description("Path for input file(s).")
  @Validation.Required
  String getInputFile();
  void setInputFile(String inputFile);

  @Description("Search pattern.")
  @Validation.Required
  String getSearchPattern();
  void setSearchPattern(String pattern);
}
