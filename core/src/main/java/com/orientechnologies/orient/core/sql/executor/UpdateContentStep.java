package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.parser.OJson;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 09/08/16.
 */
public class UpdateContentStep extends AbstractExecutionStep {
  private final OJson json;

  public UpdateContentStep(OJson json, OCommandContext ctx) {
    super(ctx);
    this.json = json;

  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OTodoResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult result = upstream.next();
        if (result instanceof OResultInternal) {
          if (!(result.getElement().get() instanceof ODocument)) {
            ((OResultInternal) result).setElement(result.getElement().get().getRecord());
          }
          if (!(result.getElement().get() instanceof ODocument)) {
            return result;
          }
          handleContent((ODocument) result.getElement().get(), ctx);
        }
        return result;
      }

      @Override public void close() {
        upstream.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  private boolean handleContent(ODocument record, OCommandContext ctx) {
    boolean updated = false;

    // REPLACE ALL THE CONTENT
    final ODocument fieldsToPreserve = new ODocument();

    final OClass restricted = ctx.getDatabase().getMetadata().getSchema().getClass(OSecurity.RESTRICTED_CLASSNAME);

    if (restricted != null && restricted.isSuperClassOf(record.getSchemaClass())) {
      for (OProperty prop : restricted.properties()) {
        fieldsToPreserve.field(prop.getName(), record.<Object>field(prop.getName()));
      }
    }

    OClass recordClass = ODocumentInternal.getImmutableSchemaClass(record);
    if (recordClass != null && recordClass.isSubClassOf("V")) {
      for (String fieldName : record.fieldNames()) {
        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
          fieldsToPreserve.field(fieldName, record.<Object>field(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
      for (String fieldName : record.fieldNames()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          fieldsToPreserve.field(fieldName, record.<Object>field(fieldName));
        }
      }
    }
    record.merge(fieldsToPreserve, false, false);
    record.merge(json.toDocument(record, ctx), true, false);

    updated = true;

    return updated;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    result.append(json);
    return result.toString();
  }
}
