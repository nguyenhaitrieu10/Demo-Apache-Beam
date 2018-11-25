package org.apache.beam.examples.complete.game;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.complete.game.utils.WriteToBigQuery;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class LeaderBoard extends HourlyTeamScore {

  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  interface Options extends HourlyTeamScore.Options, ExampleOptions, StreamingOptions {

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();
    void setTopic(String value);

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(60)
    Integer getTeamWindowDuration();
    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    Integer getAllowedLateness();
    void setAllowedLateness(Integer value);

    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    String getLeaderBoardTableName();
    void setLeaderBoardTableName(String value);
  }

  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
      configureWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>();
    tableConfigure.put(
        "team",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "window_start",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING",
            (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return fmt.print(window.start());
            }));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> fmt.print(Instant.now())));
    tableConfigure.put(
        "timing",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.pane().getTiming().toString()));
    return tableConfigure;
  }

  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
  configureBigQueryWrite() {
    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>();
    tableConfigure.put(
        "user",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "INTEGER", (c, w) -> c.element().getValue()));
    return tableConfigure;
  }

  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
      configureGlobalWindowBigQueryWrite() {

    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        configureBigQueryWrite();
    tableConfigure.put(
        "processing_time",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", (c, w) -> fmt.print(Instant.now())));
    return tableConfigure;
  }


  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<GameActionInfo> gameEvents = pipeline
        .apply(PubsubIO.readStrings()
            .withTimestampAttribute(TIMESTAMP_ATTRIBUTE).fromTopic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));

    gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        .apply(
            "WriteTeamScoreSums",
            new WriteWindowedToBigQuery<KV<String, Integer>>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_team",
                configureWindowedTableWrite()));
    gameEvents
        .apply(
            "CalculateUserScores",
            new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness())))
        .apply(
            "WriteUserScoreSums",
            new WriteToBigQuery<KV<String, Integer>>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_user",
                configureGlobalWindowBigQueryWrite()));

    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }

  @VisibleForTesting
  static class CalculateTeamScores
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
      return infos.apply("LeaderboardTeamFixedWindows",
          Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
              .triggering(AfterWatermark.pastEndOfWindow()
                  .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                      .plusDelayOf(FIVE_MINUTES))
                  .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                      .plusDelayOf(TEN_MINUTES)))
              .withAllowedLateness(allowedLateness)
              .accumulatingFiredPanes())
          .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
    }
  }
  @VisibleForTesting
  static class CalculateUserScores
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
      return input.apply("LeaderboardUserGlobalWindow",
          Window.<GameActionInfo>into(new GlobalWindows())
              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(TEN_MINUTES)))
              .accumulatingFiredPanes()
              .withAllowedLateness(allowedLateness))
          .apply("ExtractUserScore", new ExtractAndSumScore("user"));
    }
  }
}
