package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 03/08/16. */
public class LetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OStatement query;

  public LetQueryStep(OIdentifier varName, OStatement query) {
    super();
    this.varName = varName;
    this.query = query;
  }

  private OResultInternal calculate(OResultInternal result, OCommandContext ctx) {
    OBasicCommandContext subCtx = new OBasicCommandContext(ctx.getDatabase());
    subCtx.setParentWithoutOverridingChild(ctx);
    OInternalExecutionPlan subExecutionPlan;
    // with positional parameters, you cannot know if a parameter has the same ordinal as the
    // one cached
    boolean useCache = !query.toString().contains("?");
    subExecutionPlan = query.resolvePlan(useCache, subCtx);
    result.setMetadata(varName.getStringValue(), toList(subExecutionPlan, ctx));
    return result;
  }

  private List<OResult> toList(OInternalExecutionPlan plan, OCommandContext ctx) {
    OExecutionStream stream = plan.start(ctx);
    List<OResult> result = new ArrayList<>();
    while (stream.hasNext(ctx)) {
      result.add(stream.next(ctx));
    }
    stream.close(ctx);
    return result;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (!getPrev().isPresent()) {
      throw new OCommandExecutionException(
          "Cannot execute a local LET on a query without a target");
    }
    return getPrev().get().start(ctx).map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    return calculate((OResultInternal) result, ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = (" + query + ")";
  }
}
