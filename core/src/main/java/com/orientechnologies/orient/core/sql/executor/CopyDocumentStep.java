package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;

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
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx);
    return new OResultSetMapper(upstream, this::mapResult);
  }

  private OResult mapResult(OResult result) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      ORecord resultDoc = null;
      if (result.isElement()) {
        ORecord docToCopy = result.getElement().get().getRecord();
        if (docToCopy instanceof ODocument) {
          resultDoc = ((ODocument) docToCopy).copy();
          resultDoc.getIdentity().reset();
          ((ODocument) resultDoc).setClassName(null);
          resultDoc.setDirty();
        } else if (docToCopy instanceof OBlob) {
          ORecordBytes newBlob = ((ORecordBytes) docToCopy).copy();
          OResultInternal newResult = new OResultInternal(newBlob);
          return newResult;
        }
      } else {
        resultDoc = result.toElement().getRecord();
      }
      return new OUpdatableResult((ODocument) resultDoc);
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
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
