package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OIfStatement;
import com.orientechnologies.orient.core.sql.parser.OReturnStatement;
import com.orientechnologies.orient.core.sql.parser.OStatement;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 *     <p>This step represents the execution plan of an instruciton instide a batch script
 */
public class ScriptLineStep extends AbstractExecutionStep {
  protected final OInternalExecutionPlan plan;

  private boolean executed = false;

  public ScriptLineStep(
      OInternalExecutionPlan nextPlan, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.plan = nextPlan;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!executed) {
      if (plan instanceof OInsertExecutionPlan) {
        ((OInsertExecutionPlan) plan).executeInternal();
      } else if (plan instanceof ODeleteExecutionPlan) {
        ((ODeleteExecutionPlan) plan).executeInternal();
      } else if (plan instanceof OUpdateExecutionPlan) {
        ((OUpdateExecutionPlan) plan).executeInternal();
      } else if (plan instanceof ODDLExecutionPlan) {
        ((ODDLExecutionPlan) plan).executeInternal((OBasicCommandContext) ctx);
      } else if (plan instanceof OSingleOpExecutionPlan) {
        ((OSingleOpExecutionPlan) plan).executeInternal((OBasicCommandContext) ctx);
      }
      executed = true;
    }
    return plan.fetchNext(nRecords);
  }

  public boolean containsReturn() {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).containsReturn();
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return true;
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      IfStep step = (IfStep) plan.getSteps().get(0);
      if (step.positivePlan != null && step.positivePlan.containsReturn()) {
        return true;
      } else if (step.positiveStatements != null) {
        for (OStatement stm : step.positiveStatements) {
          if (containsReturn(stm)) {
            return true;
          }
        }
      }
    }

    if (plan instanceof OForEachExecutionPlan) {
      if (((OForEachExecutionPlan) plan).containsReturn()) {
        return true;
      }
    }
    return false;
  }

  private boolean containsReturn(OStatement stm) {
    if (stm instanceof OReturnStatement) {
      return true;
    }
    if (stm instanceof OIfStatement) {
      for (OStatement o : ((OIfStatement) stm).getStatements()) {
        if (containsReturn(o)) {
          return true;
        }
      }
    }
    return false;
  }

  public OExecutionStepInternal executeUntilReturn(OCommandContext ctx) {
    if (plan instanceof OScriptExecutionPlan) {
      return ((OScriptExecutionPlan) plan).executeUntilReturn();
    }
    if (plan instanceof OSingleOpExecutionPlan) {
      if (((OSingleOpExecutionPlan) plan).statement instanceof OReturnStatement) {
        return new ReturnStep(((OSingleOpExecutionPlan) plan).statement, ctx, profilingEnabled);
      }
    }
    if (plan instanceof OIfExecutionPlan) {
      return ((OIfExecutionPlan) plan).executeUntilReturn();
    }
    throw new IllegalStateException();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (plan == null) {
      return "Script Line";
    }
    return plan.prettyPrint(depth, indent);
  }
}
