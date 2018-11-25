package org.apache.beam.examples.complete.game;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.beam.examples.complete.game.utils.WriteToText;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class HourlyTeamScore extends UserScore {

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  private static DateTimeFormatter minFmt =
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));

  interface Options extends UserScore.Options {

    @Description("Numeric value of fixed window duration, in minutes")
    @Default.Integer(60)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);

    @Description("String representation of the first minute after which to generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped prior to that minute won't be included in the sums.")
    @Default.String("1970-01-01-00-00")
    String getStartMin();
    void setStartMin(String value);

    @Description("String representation of the first minute for which to not generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped after that minute won't be included in the sums.")
    @Default.String("2100-01-01-00-00")
    String getStopMin();
    void setStopMin(String value);
  }

  protected static Map<String, WriteToText.FieldFn<KV<String, Integer>>>
      configureOutput() {
    Map<String, WriteToText.FieldFn<KV<String, Integer>>> config =
        new HashMap<String, WriteToText.FieldFn<KV<String, Integer>>>();
    config.put("team", (c, w) -> c.element().getKey());
    config.put("total_score", (c, w) -> c.element().getValue());
    config.put(
        "window_start",
        (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return fmt.print(window.start());
            });
    return config;
  }


  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    final Instant stopMinTimestamp = new Instant(minFmt.parseMillis(options.getStopMin()));
    final Instant startMinTimestamp = new Instant(minFmt.parseMillis(options.getStartMin()));

    pipeline.apply(TextIO.read().from(options.getInput()))
      .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))

      .apply("FilterStartTime", Filter.by(
          (GameActionInfo gInfo)
              -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
      .apply("FilterEndTime", Filter.by(
          (GameActionInfo gInfo)
              -> gInfo.getTimestamp() < stopMinTimestamp.getMillis()))
      .apply("AddEventTimestamps",
             WithTimestamps.of((GameActionInfo i) -> new Instant(i.getTimestamp())))
      .apply("FixedWindowsTeam", Window.<GameActionInfo>into(
          FixedWindows.of(Duration.standardMinutes(options.getWindowDuration()))))

      .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
      .apply("WriteTeamScoreSums",
          new WriteToText<KV<String, Integer>>(
              options.getOutput(),
              configureOutput(),
              true));

    pipeline.run().waitUntilFinish();
  }
}
