package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchFirstStep extends AbstractExecutionStep {
  private final PatternNode node;
  Iterator<OIdentifiable> iterator;

  public MatchFirstStep(OCommandContext context, PatternNode node) {
    super(context);
    this.node = node;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x->x.syncPull(ctx, nRecords));
    init(ctx);
    return new OTodoResultSet() {

      @Override public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override public OResult next() {
        OResultInternal result = new OResultInternal();
        result.setProperty(getAlias(), iterator.next());
        ctx.setVariable("$matched", result);
        return result;
      }

      @Override public void close() {

      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  private void init(OCommandContext ctx) {
    if (iterator == null) {
      String alias = getAlias();
      Object matchedNodes = ctx.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        Iterable possibleResults;
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
        iterator = possibleResults.iterator();
      } else {
        iterator = Collections.emptyIterator();//TODO
      }
    }
  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ SET \n");
    result.append(spaces);
    result.append("  ");
    result.append(getAlias());

    return result.toString();
  }

  private String getAlias() {
    return this.node.alias;
  }

  @Override public void sendResult(Object o, Status status) {

  }
}
