package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/05/17.
 */
public class DistributedExecutionStep extends AbstractExecutionStep {

  private final OInternalExecutionPlan subExecuitonPlan;
  private final String                 nodeName;

  private boolean inited;

  public DistributedExecutionStep(OInternalExecutionPlan subExecutionPlan, String nodeName, OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.subExecuitonPlan = subExecutionPlan;
    this.nodeName = nodeName;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return false;//TODO
      }

      @Override
      public OResult next() {
        return null;//TODO
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
      //TODO serialize and send the sub-execution plan to the remote node
    }

  }

  @Override
  public void close() {
    super.close();
    //TODO close the remote connection and execution
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
