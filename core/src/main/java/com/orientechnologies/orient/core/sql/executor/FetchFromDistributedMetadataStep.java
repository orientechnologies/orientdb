package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceExecutionStream;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromDistributedMetadataStep extends AbstractExecutionStep {

  public FetchFromDistributedMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return new OProduceExecutionStream(this::produce).limit(1);
  }

  private OResult produce(OCommandContext ctx) {
    ODatabaseDocumentInternal session = (ODatabaseDocumentInternal) ctx.getDatabase();
    OSharedContextEmbedded value = (OSharedContextEmbedded) session.getSharedContext();
    ODocument doc = value.loadDistributedConfig(session);
    OResultInternal result = new OResultInternal();
    doc.setTrackingChanges(false);
    doc.deserializeFields();

    for (String alias : doc.getPropertyNames()) {
      result.setProperty(alias, doc.getProperty(alias));
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
