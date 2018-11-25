package org.apache.beam.examples.complete.game.utils;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class WriteWindowedToBigQuery<T>
    extends WriteToBigQuery<T> {

  public WriteWindowedToBigQuery(
      String projectId, String datasetId, String tableName, Map<String, FieldInfo<T>> fieldInfo) {
    super(projectId, datasetId, tableName, fieldInfo);
  }

  protected class BuildRowFn extends DoFn<T, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {

      TableRow row = new TableRow();
      for (Map.Entry<String, FieldInfo<T>> entry : fieldInfo.entrySet()) {
          String key = entry.getKey();
          FieldInfo<T> fcnInfo = entry.getValue();
          row.set(key, fcnInfo.getFieldFn().apply(c, window));
        }
      c.output(row);
    }
  }

  @Override
  public PDone expand(PCollection<T> teamAndScore) {
    teamAndScore
      .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
      .apply(BigQueryIO.writeTableRows()
                .to(getTable(projectId, datasetId, tableName))
                .withSchema(getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    return PDone.in(teamAndScore.getPipeline());
  }

}
