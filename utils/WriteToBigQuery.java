package org.apache.beam.examples.complete.game.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class WriteToBigQuery<InputT>
    extends PTransform<PCollection<InputT>, PDone> {

  protected String projectId;
  protected String datasetId;
  protected String tableName;
  protected Map<String, FieldInfo<InputT>> fieldInfo;

  public WriteToBigQuery() {
  }

  public WriteToBigQuery(
      String projectId,
      String datasetId,
      String tableName,
      Map<String, FieldInfo<InputT>> fieldInfo) {
    this.projectId = projectId;
    this.datasetId = datasetId;
    this.tableName = tableName;
    this.fieldInfo = fieldInfo;
  }

  public interface FieldFn<InputT> extends Serializable {
    Object apply(DoFn<InputT, TableRow>.ProcessContext context, BoundedWindow window);
  }

  public static class FieldInfo<InputT> implements Serializable {
    
    private String fieldType;
    
    private FieldFn<InputT> fieldFn;

    public FieldInfo(String fieldType,
        FieldFn<InputT> fieldFn) {
      this.fieldType = fieldType;
      this.fieldFn = fieldFn;
    }

    String getFieldType() {
      return this.fieldType;
    }

    FieldFn<InputT> getFieldFn() {
      return this.fieldFn;
    }
  }
  protected class BuildRowFn extends DoFn<InputT, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) {

      TableRow row = new TableRow();
      for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
          String key = entry.getKey();
          FieldInfo<InputT> fcnInfo = entry.getValue();
          FieldFn<InputT> fcn = fcnInfo.getFieldFn();
          row.set(key, fcn.apply(c, window));
        }
      c.output(row);
    }
  }


  protected TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    for (Map.Entry<String, FieldInfo<InputT>> entry : fieldInfo.entrySet()) {
      String key = entry.getKey();
      FieldInfo<InputT> fcnInfo = entry.getValue();
      String bqType = fcnInfo.getFieldType();
      fields.add(new TableFieldSchema().setName(key).setType(bqType));
    }
    return new TableSchema().setFields(fields);
  }

  @Override
  public PDone expand(PCollection<InputT> teamAndScore) {
    teamAndScore
        .apply("ConvertToRow", ParDo.of(new BuildRowFn()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(getTable(projectId, datasetId, tableName))
                .withSchema(getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    return PDone.in(teamAndScore.getPipeline());
  }

  static TableReference getTable(String projectId, String datasetId, String tableName) {
    TableReference table = new TableReference();
    table.setDatasetId(datasetId);
    table.setProjectId(projectId);
    table.setTableId(tableName);
    return table;
  }
}
