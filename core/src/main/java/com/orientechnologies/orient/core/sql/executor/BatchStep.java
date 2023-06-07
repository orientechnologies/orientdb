package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.resultset.OResultSetMapper;
import com.orientechnologies.orient.core.sql.parser.OBatch;

/** Created by luigidellaquila on 14/02/17. */
public class BatchStep extends AbstractExecutionStep {

  private Integer batchSize;
  private int count = 0;

  public BatchStep(OBatch batch, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    batchSize = batch.evaluate(ctx);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet prevResult = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSetMapper(prevResult, (result) -> mapResult(ctx, result));
  }

  private OResult mapResult(OCommandContext ctx, OResult result) {
    if (count % batchSize == 0) {
      ODatabaseSession db = ctx.getDatabase();
      if (db.getTransaction().isActive()) {
        db.commit();
        db.begin();
      }
    }
    count++;
    return result;
  }

  @Override
  public void reset() {
    this.count = 0;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BATCH COMMIT EVERY " + batchSize);
    return result.toString();
  }
}
