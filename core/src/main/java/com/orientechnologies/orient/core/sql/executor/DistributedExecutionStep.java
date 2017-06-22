package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/05/17.
 */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OSelectExecutionPlan subExecuitonPlan;
  private final String               nodeName;

  private boolean inited;

  private OResultSet remoteResultSet;

  public DistributedExecutionStep(OSelectExecutionPlan subExecutionPlan, String nodeName, OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    init(ctx);
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        throw new UnsupportedOperationException("Implement distributed execution step!");
      }

      @Override
      public OResult next() {
        throw new UnsupportedOperationException("Implement distributed execution step!");
      }

      @Override
      public void close() {
        DistributedExecutionStep.this.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  public void init(OCommandContext ctx) {
    if (!inited) {
      inited = true;
      OResult serializedExecutionPlan = subExecuitonPlan.toResult();
      this.remoteResultSet = sendSerializedExecutionPlan(nodeName, serializedExecutionPlan, ctx);
    }
  }

  private OResultSet sendSerializedExecutionPlan(String nodeName, OResult serializedExecutionPlan, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    return db.queryOnNode(nodeName, serializedExecutionPlan, ctx.getInputParameters());
  }

  @Override
  public void close() {
    super.close();
    if (this.remoteResultSet != null) {
      this.remoteResultSet.close();
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(depth, indent);
    builder.append(ind);
    builder.append("+ EXECUTE ON NODE " + nodeName + "----------- \n");
    builder.append(subExecuitonPlan.prettyPrint(depth + 1, indent));
    builder.append("  ------------------------------------------- \n");
    builder.append("   |\n");
    builder.append("   V\n");
    return builder.toString();
  }
}
