package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** Created by luigidellaquila on 08/05/17. */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OSelectExecutionPlan subExecuitonPlan;
  private final String nodeName;

  public DistributedExecutionStep(OSelectExecutionPlan subExecutionPlan, String nodeName) {
    super();
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream remote = sendSerializedExecutionPlan(nodeName, subExecuitonPlan, ctx);
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    return remote;
  }

  private OExecutionStream sendSerializedExecutionPlan(
      String nodeName, OInternalExecutionPlan serializedExecutionPlan, OCommandContext ctx) {
    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) ctx.getDatabase();
    return db.queryOnNode(nodeName, serializedExecutionPlan, ctx.getInputParameters());
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    StringBuilder builder = new StringBuilder();
    String ind = OExecutionStepInternal.getIndent(ctx);
    builder.append(ind);
    builder.append("+ EXECUTE ON NODE " + nodeName + "----------- \n");
    ctx.incDepth();
    builder.append(subExecuitonPlan.prettyPrint(ctx));
    ctx.decDepth();
    builder.append("  ------------------------------------------- \n");
    builder.append("   |\n");
    builder.append("   V\n");
    return builder.toString();
  }
}
