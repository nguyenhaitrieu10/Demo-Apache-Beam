package org.apache.beam.examples.complete.game;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.complete.game.utils.WriteToText;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserScore {

  @DefaultCoder(AvroCoder.class)
  static class GameActionInfo {
    @Nullable String user;
    @Nullable String team;
    @Nullable Integer score;
    @Nullable Long timestamp;

    public GameActionInfo() {}

    public GameActionInfo(String user, String team, Integer score, Long timestamp) {
      this.user = user;
      this.team = team;
      this.score = score;
      this.timestamp = timestamp;
    }

    public String getUser() {
      return this.user;
    }
    public String getTeam() {
      return this.team;
    }
    public Integer getScore() {
      return this.score;
    }
    public String getKey(String keyname) {
      if (keyname.equals("team")) {
        return this.team;
      } else {  
        return this.user;
      }
    }
    public Long getTimestamp() {
      return this.timestamp;
    }
  }

  static class ParseEventFn extends DoFn<String, GameActionInfo> {

    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",");
      try {
        String user = components[0].trim();
        String team = components[1].trim();
        Integer score = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[3].trim());
        GameActionInfo gInfo = new GameActionInfo(user, team, score, timestamp);
        c.output(gInfo);
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.inc();
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }

  public static class ExtractAndSumScore
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

    private final String field;

    ExtractAndSumScore(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(
        PCollection<GameActionInfo> gameInfo) {

      return gameInfo
        .apply(MapElements
            .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
            .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
        .apply(Sum.<String>integersPerKey());
    }
  }
  
  public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing game data.")
    @Default.String("gs://apache-beam-samples/game/gaming_data*.csv")
    String getInput();
    void setInput(String value);

    @Description("Path of the file to write to.")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>>
      configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Integer>>> config =
        new HashMap<String, WriteToText.FieldFn<KV<String, Integer>>>();
    config.put("user", (c, w) -> c.element().getKey());
    config.put("total_score", (c, w) -> c.element().getValue());
    return config;
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(TextIO.read().from(options.getInput()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
        .apply(
            "WriteUserScoreSums",
            new WriteToText<KV<String, Integer>>(
                options.getOutput(),
                configureOutput(),
                false));

    pipeline.run().waitUntilFinish();
  }
}
