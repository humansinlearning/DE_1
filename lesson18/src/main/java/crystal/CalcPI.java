package crystal;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

public class CalcPI {
  private static final Logger __logger = LoggerFactory.getLogger(CalcPI.class);

  public interface CalcPIOptions extends PipelineOptions {
    @Default.Long(100_000)
    @Description("Give me a precision")
    Long getIterationCount();

    void setIterationCount(Long value);
  }

  private static boolean isHit() {
    return Stream.of(-1 + 2 * Math.random(), -1 + 2 * Math.random())
        .map(it -> it * it)
        .reduce(0.0, Double::sum)
        <= 1.0;
  }

  private static void piSQLWay(CalcPIOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    Schema simpleBoolSchema = Schema.builder().addBooleanField("hit").build();

    pipeline
        .apply("Generate sequence", GenerateSequence.from(0).to(options.getIterationCount()))
        .apply(
            "Generate hits", MapElements.into(TypeDescriptors.booleans()).via(element -> isHit()))
        .apply(
            "Convert to rows",
            MapElements.into(TypeDescriptors.rows())
                .via(el -> Row.withSchema(simpleBoolSchema).addValues(el).build()))
        .setRowSchema(simpleBoolSchema)
        .apply(
            "Calculate partial hit counts",
            SqlTransform.query("SELECT count(*) as hits FROM PCOLLECTION WHERE hit"))
        .setCoder(RowCoder.of(Schema.builder().addInt64Field("hits").build()))
        .apply("Convert ROW to LONG", Convert.fromRows(Long.class))
        //.apply("Sum all", Sum.longsGlobally())
        .apply(
            "Calc PI",
            ParDo.of(
                new DoFn<Long, Double>() {
                  @ProcessElement
                  public void processElement(@Element Long input, ProcessContext ctx) {
                    CalcPIOptions opt = ctx.getPipelineOptions().as(CalcPIOptions.class);
                    double approxPI = 4.0 * input / (opt.getIterationCount() * 1.0);
                    double relError = Math.abs(approxPI - Math.PI) / Math.PI;
                    String result =
                        String.format(
                            "^^^^^^^Approx PI is %f, Math lib PI is %f with  error  %f%%",
                            approxPI, Math.PI, 100 * relError);
                    System.out.println(result);
                    ctx.output(approxPI);
                  }
                }));

    pipeline.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    CalcPIOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(CalcPIOptions.class);
    piSQLWay(options);
  }
}
