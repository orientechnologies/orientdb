package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class RemoveEmptyOptionalsStep extends AbstractExecutionStep {

  public RemoveEmptyOptionalsStep(OIdentifier cluster) {
    super();
  }

  public RemoveEmptyOptionalsStep() {
    this(null);
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
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ REMOVE EMPTY OPTIONALS");
    return result.toString();
  }
}
