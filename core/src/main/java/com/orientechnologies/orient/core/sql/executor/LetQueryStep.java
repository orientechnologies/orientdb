package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 03/08/16. */
public class LetQueryStep extends AbstractExecutionStep {

  private final OIdentifier varName;
  private final OStatement query;

  public LetQueryStep(
      OIdentifier varName, OStatement query, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.varName = varName;
    this.query = query;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!getPrev().isPresent()) {
      throw new OCommandExecutionException(
          "Cannot execute a local LET on a query without a target");
    }
    return new OResultSet() {
      private OResultSet source = getPrev().get().syncPull(ctx, nRecords);

      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public OResult next() {
        OResultInternal result = (OResultInternal) source.next();
        if (result != null) {
          calculate(result, ctx);
        }
        return result;
      }

      private void calculate(OResultInternal result, OCommandContext ctx) {
        OBasicCommandContext subCtx = new OBasicCommandContext();
        subCtx.setDatabase(ctx.getDatabase());
        subCtx.setParentWithoutOverridingChild(ctx);
        OInternalExecutionPlan subExecutionPlan;
        if (query.toString().contains("?")) {
          // with positional parameters, you cannot know if a parameter has the same ordinal as the
          // one cached
          subExecutionPlan = query.createExecutionPlanNoCache(subCtx, profilingEnabled);
        } else {
          subExecutionPlan = query.createExecutionPlan(subCtx, profilingEnabled);
        }
        result.setMetadata(varName.getStringValue(), toList(new OLocalResultSet(subExecutionPlan)));
      }

      private List<OResult> toList(OLocalResultSet oLocalResultSet) {
        List<OResult> result = new ArrayList<>();
        while (oLocalResultSet.hasNext()) {
          result.add(oLocalResultSet.next());
        }
        oLocalResultSet.close();
        return result;
      }

      @Override
      public void close() {
        source.close();
      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ LET (for each record)\n" + spaces + "  " + varName + " = (" + query + ")";
  }
}
