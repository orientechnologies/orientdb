package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 20/09/16. */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode node;
  private OInternalExecutionPlan executionPlan;

  private Iterator<OResult> iterator;
  private OResultSet subResultSet;

  public MatchFirstStep(OCommandContext context, PatternNode node, boolean profilingEnabled) {
    this(context, node, null, profilingEnabled);
  }

  public MatchFirstStep(
      OCommandContext context,
      PatternNode node,
      OInternalExecutionPlan subPlan,
      boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.node = node;
    this.executionPlan = subPlan;
  }

  @Override
  public void reset() {
    this.iterator = null;
    this.subResultSet = null;
    if (executionPlan != null) {
      executionPlan.reset(this.getContext());
    }
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init(ctx);
    return new OResultSet() {

      private int currentCount = 0;

      @Override
      public boolean hasNext() {
        if (currentCount >= nRecords) {
          return false;
        }
        if (iterator != null) {
          return iterator.hasNext();
        } else {
          return subResultSet.hasNext();
        }
      }

      @Override
      public OResult next() {
        if (currentCount >= nRecords) {
          throw new IllegalStateException();
        }
        OResultInternal result = new OResultInternal();
        if (iterator != null) {
          result.setProperty(getAlias(), iterator.next());
        } else {
          result.setProperty(getAlias(), subResultSet.next());
        }
        ctx.setVariable("$matched", result);
        currentCount++;
        return result;
      }

      @Override
      public void close() {}

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

  private Object toResult(OIdentifiable nextElement) {
    return new OResultInternal(nextElement);
  }

  private void init(OCommandContext ctx) {
    if (iterator == null && subResultSet == null) {
      String alias = getAlias();
      Object matchedNodes =
          ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        initFromPrefetch(matchedNodes);
      } else {
        initFromExecutionPlan(ctx);
      }
    }
  }

  private void initFromExecutionPlan(OCommandContext ctx) {
    this.subResultSet = new OLocalResultSet(executionPlan);
  }

  private void initFromPrefetch(Object matchedNodes) {
    Iterable possibleResults;
    if (matchedNodes instanceof Iterable) {
      possibleResults = (Iterable) matchedNodes;
    } else {
      possibleResults = Collections.singleton(matchedNodes);
    }
    iterator = possibleResults.iterator();
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET \n");
    result.append(spaces);
    result.append("   ");
    result.append(getAlias());
    if (executionPlan != null) {
      result.append("\n");
      result.append(spaces);
      result.append("  AS\n");
      result.append(executionPlan.prettyPrint(depth + 1, indent));
    }

    return result.toString();
  }

  private String getAlias() {
    return this.node.alias;
  }
}
