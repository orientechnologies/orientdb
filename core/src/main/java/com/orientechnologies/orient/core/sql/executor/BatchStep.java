package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBatch;

/** Created by luigidellaquila on 14/02/17. */
public class BatchStep extends AbstractExecutionStep {

  private Integer batchSize;

  public BatchStep(OBatch batch, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    batchSize = batch.evaluate(ctx);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream prevResult = getPrev().get().syncPull(ctx);
    return prevResult.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    ODatabaseSession db = ctx.getDatabase();
    if (db.getTransaction().isActive()) {
      if (db.getTransaction().getEntryCount() % batchSize == 0) {
        db.commit();
        db.begin();
      }
    }
    return result;
  }

  @Override
  public void reset() {}

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BATCH COMMIT EVERY " + batchSize);
    return result.toString();
  }
}
