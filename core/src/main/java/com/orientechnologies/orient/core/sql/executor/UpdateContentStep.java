package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OJson;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 09/08/16. */
public class UpdateContentStep extends AbstractExecutionStep {
  private OJson json;
  private OInputParameter inputParameter;

  public UpdateContentStep(OJson json, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  public UpdateContentStep(
      OInputParameter inputParameter, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.inputParameter = inputParameter;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        if (result instanceof OResultInternal) {
          if (!(result.getElement().get() instanceof OElement)) {
            ((OResultInternal) result).setElement(result.getElement().get().getRecord());
          }
          if (!(result.getElement().get() instanceof OElement)) {
            return result;
          }
          handleContent((OElement) result.getElement().get(), ctx);
        }
        return result;
      }

      @Override
      public void close() {
        upstream.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  private boolean handleContent(OElement record, OCommandContext ctx) {
    boolean updated = false;

    // REPLACE ALL THE CONTENT
    final ODocument fieldsToPreserve = new ODocument();

    final OClass restricted =
        ((ODatabaseDocumentInternal) ctx.getDatabase())
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClass(OSecurity.RESTRICTED_CLASSNAME);

    if (restricted != null && restricted.isSuperClassOf(record.getSchemaType().orElse(null))) {
      for (OProperty prop : restricted.properties()) {
        fieldsToPreserve.field(prop.getName(), record.<Object>getProperty(prop.getName()));
      }
    }

    OClass recordClass =
        ODocumentInternal.getImmutableSchemaClass(
            (ODatabaseDocumentInternal) ctx.getDatabase(), record.getRecord());
    if (recordClass != null && recordClass.isSubClassOf("V")) {
      for (String fieldName : record.getPropertyNames()) {
        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
          fieldsToPreserve.field(fieldName, record.<Object>getProperty(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
      for (String fieldName : record.getPropertyNames()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          fieldsToPreserve.field(fieldName, record.<Object>getProperty(fieldName));
        }
      }
    }
    ODocument doc = record.getRecord();
    if (json != null) {
      doc.merge(json.toDocument(record, ctx), false, false);
    } else if (inputParameter != null) {
      Object val = inputParameter.getValue(ctx.getInputParameters());
      if (val instanceof OElement) {
        doc.merge((ODocument) ((OElement) val).getRecord(), false, false);
      } else if (val instanceof Map) {
        doc.merge(new ODocument().fromMap((Map) val), false, false);
      } else {
        throw new OCommandExecutionException("Invalid value for UPDATE CONTENT: " + val);
      }
    }
    doc.merge(fieldsToPreserve, true, false);

    updated = true;

    return updated;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    if (json != null) {
      result.append(json);
    } else {
      result.append(inputParameter);
    }
    return result.toString();
  }
}
