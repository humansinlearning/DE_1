package crystal;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.File;
import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.Arrays;

@Slf4j
public class SparkJob1 implements Serializable {
  public static void main(String[] args) {
    JobOptions options = PipelineOptionsFactory.fromArgs(args).as(JobOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info("Pipeline options: {}", options);
    String searchPattern = options.getSearchPattern().toUpperCase();
    PCollection<KV<String, Long>> countPColl = pipeline.apply("Read data", TextIO.read()
            .from("/opt/spark-data/" + options.getInputFile())
            .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
        .apply("ExtractWords", FlatMapElements
            .into(TypeDescriptors.strings())
            .via((String line) -> {
              return Arrays.asList(line.split("[^\\p{L}]+"));
            }))
        .apply("To Uppercase", MapElements.into(TypeDescriptors.strings()).via((SerializableFunction<String, String>) input -> input.toUpperCase()))
        .apply("Filter by pattern", Filter.by(s -> s.contains(searchPattern)))
        .apply(Count.<String>perElement());
    countPColl.apply("Write to jdbc", JdbcIO.<KV<String, Long>>write()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create("org.postgresql.Driver", "jdbc:postgresql://demo-database:5432/postgres")
                .withPassword("casa1234")
                .withUsername("postgres")
        )
        .withStatement("INSERT INTO spark.wc(word,count) values (?,?)")
        .withPreparedStatementSetter(new KVPreparedStatementSetter()));
    pipeline.run(options);
  }

  private static class KVPreparedStatementSetter implements JdbcIO.PreparedStatementSetter<KV<String, Long>> {
    @Override
    public void setParameters(KV<String, Long> element, PreparedStatement preparedStatement) throws Exception {
      preparedStatement.setString(1, element.getKey());
      preparedStatement.setLong(2, element.getValue());
    }
  }
}