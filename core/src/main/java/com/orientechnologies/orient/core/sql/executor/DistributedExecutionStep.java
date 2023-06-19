package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

/** Created by luigidellaquila on 08/05/17. */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OSelectExecutionPlan subExecuitonPlan;
  private final String nodeName;

  public DistributedExecutionStep(
      OSelectExecutionPlan subExecutionPlan,
      String nodeName,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    OResultSet remote = sendSerializedExecutionPlan(nodeName, subExecuitonPlan, ctx);
    getPrev().ifPresent(x -> x.syncPull(ctx));
    return remote;
  }

  private OResultSet sendSerializedExecutionPlan(
      String nodeName, OExecutionPlan serializedExecutionPlan, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    return db.queryOnNode(nodeName, serializedExecutionPlan, ctx.getInputParameters());
  }

  @Override
  public void close() {
    super.close();
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
