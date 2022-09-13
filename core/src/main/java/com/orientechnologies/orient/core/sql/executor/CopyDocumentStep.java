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
 * Reads an upstream result set and returns a new result set that contains copies of the original
 * OResult instances
 *
 * <p>This is mainly used from statements that need to copy of the original data to save it
 * somewhere else, eg. INSERT ... FROM SELECT
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class CopyDocumentStep extends AbstractExecutionStep {

  private long cost = 0;

  public CopyDocumentStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
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
        OResult toCopy = upstream.next();
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
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
              OResultInternal result = new OResultInternal(newBlob);
              return result;
            }
          } else {
            resultDoc = toCopy.toElement().getRecord();
          }
          return new OUpdatableResult((ODocument) resultDoc);
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COPY DOCUMENT");
    if (profilingEnabled) {
      result.append(" (" + getCostFormatted() + ")");
    }
    return result.toString();
  }

  @Override
  public long getCost() {
    return cost;
  }
}
