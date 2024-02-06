package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class RemoveEmptyOptionalsStep extends AbstractExecutionStep {

  public RemoveEmptyOptionalsStep(
      OCommandContext ctx, OIdentifier cluster, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  public RemoveEmptyOptionalsStep(OCommandContext ctx, boolean profilingEnabled) {
    this(ctx, null, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    OExecutionStream upstream = getPrev().get().start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    for (String s : result.getPropertyNames()) {
      if (OptionalMatchEdgeTraverser.isEmptyOptional(result.getProperty(s))) {
        ((OResultInternal) result).setProperty(s, null);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ REMOVE EMPTY OPTIONALS");
    return result.toString();
  }
}
