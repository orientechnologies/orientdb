package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 11/08/16.
 */
public class CopyDocumentStep extends AbstractExecutionStep {
  public CopyDocumentStep(OInsertExecutionPlan result, OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OTodoResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override public OResult next() {
        OResult toCopy = upstream.next();
        ORecord resultDoc = null;
        if (toCopy.isElement()) {
          ORecord docToCopy = toCopy.getElement().get().getRecord();
          if (docToCopy instanceof ODocument) {
            resultDoc = ((ODocument) docToCopy).copy();
            resultDoc.getIdentity().reset();
            ((ODocument) resultDoc).setClassName(null);
            resultDoc.setDirty();
          } else if (docToCopy instanceof OBlob) {
            ORecordBytes newBlob = ((ORecordBytes) docToCopy).copy();
            OResultInternal result = new OResultInternal();
            result.setElement(newBlob);
            return result;
          }
        } else {
          resultDoc = toCopy.toElement().getRecord();
        }
        return new OUpdatableResult((ODocument) resultDoc);
      }

      @Override public void close() {
        upstream.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY DOCUMENT");
    return result.toString();
  }
}
