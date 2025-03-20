package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Created by luigidellaquila on 03/08/16. */
public class GlobalLetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OInternalExecutionPlan subExecutionPlan;

  public GlobalLetQueryStep(
      OIdentifier varName, OStatement query, OCommandContext ctx, List<String> scriptVars) {
    super();
    this.varName = varName;

    OBasicCommandContext subCtx = new OBasicCommandContext(ctx.getDatabase());
    if (scriptVars != null) {
      scriptVars.forEach(x -> subCtx.declareScriptVariable(x));
    }
    subCtx.setParent(ctx);
    boolean useCache = !query.toString().contains("?");
    // with positional parameters, you cannot know if a parameter has the same ordinal as the one
    // cached
    subExecutionPlan = query.resolvePlan(useCache, subCtx);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    calculate(ctx);
    return OExecutionStream.empty();
  }

  private void calculate(OCommandContext ctx) {
    ctx.setVariable(varName.getStringValue(), toList(subExecutionPlan, ctx));
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
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
    return spaces
        + "+ LET (once)\n"
        + spaces
        + "  "
        + varName
        + " = \n"
        + box(spaces + "    ", this.subExecutionPlan.prettyPrint(ctx));
  }

  @Override
  public List<OInternalExecutionPlan> getSubExecutionPlans() {
    return Collections.singletonList(this.subExecutionPlan);
  }

  @Override
  public void serializeToResult(OResultInternal result, OToResultContext ctx) {
    result.setProperty(
        "subExecutionPlans", Collections.singletonList(this.subExecutionPlan.toResult(ctx)));
  }

  private String box(String spaces, String s) {
    String[] rows = s.split("\n");
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+-------------------------\n");
    for (String row : rows) {
      result.append(spaces);
      result.append("| ");
      result.append(row);
      result.append("\n");
    }
    result.append(spaces);
    result.append("+-------------------------");
    return result.toString();
  }
}
