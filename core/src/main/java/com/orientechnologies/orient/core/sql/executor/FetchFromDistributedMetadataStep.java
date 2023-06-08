package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OSharedContextEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OProduceOneResult;

/**
 * Returns an OResult containing metadata regarding the database
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromDistributedMetadataStep extends AbstractExecutionStep {

  private long cost = 0;
  private OResultSet resultSet = null;

  public FetchFromDistributedMetadataStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (resultSet != null) {
      getPrev().ifPresent(x -> x.syncPull(ctx));
      resultSet = new OProduceOneResult(() -> produce(ctx), true);
    }
    return resultSet;
  }

  private OResult produce(OCommandContext ctx) {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {

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
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  @Override
  public void reset() {
    this.resultSet = null;
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

  @Override
  public long getCost() {
    return cost;
  }
}
