package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

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

  public CopyDocumentStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
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
}
