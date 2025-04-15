package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexInternal;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexCandidate;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStreamProducer;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/** Created by luigidellaquila on 11/08/16. */
public class DeleteFromIndexStep extends AbstractExecutionStep {
  protected final OIndexInternal index;
  private final OBooleanExpression ridCondition;
  private final boolean orderAsc;

  private final OIndexCandidate condition;

  public DeleteFromIndexStep(
      OIndex index, OIndexCandidate condition, OBooleanExpression ridCondition) {
    this(index, condition, ridCondition, true);
  }

  private DeleteFromIndexStep(
      OIndex index, OIndexCandidate condition, OBooleanExpression ridCondition, boolean orderAsc) {
    super();
    this.index = index.getInternal();
    this.condition = condition;
    this.ridCondition = ridCondition;
    this.orderAsc = orderAsc;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));
    List<OIndexStream> streams = condition.getStreams(ctx, isOrderAsc());
    OExecutionStreamProducer res =
        new OExecutionStreamProducer() {
          private final Iterator<OIndexStream> iter = streams.iterator();

          @Override
          public OExecutionStream next(OCommandContext ctx) {
            Stream<ORawPair<Object, ORID>> s = iter.next().start(ctx);
            return OExecutionStream.resultIterator(
                s.filter(
                        (entry) -> {
                          return filter(entry, ctx);
                        })
                    .map((nextEntry) -> readResult(ctx, nextEntry))
                    .iterator());
          }

          @Override
          public boolean hasNext(OCommandContext ctx) {
            return iter.hasNext();
          }

          @Override
          public void close(OCommandContext ctx) {
            while (iter.hasNext()) {
              iter.next();
            }
          }
        };
    return OExecutionStream.multipleStreams(res);
  }

  private OResult readResult(OCommandContext ctx, ORawPair<Object, ORID> entry) {
    OResultInternal result = new OResultInternal();
    ORID value = entry.second;
    index.remove(entry.first, value);
    return result;
  }

  private boolean filter(ORawPair<Object, ORID> entry, OCommandContext ctx) {
    if (ridCondition != null) {
      OResultInternal res = new OResultInternal();
      res.setProperty("rid", entry.second);
      if (ridCondition.evaluate(res, ctx)) {
        return true;
      }
      return false;
    } else {
      return true;
    }
  }

  protected boolean isOrderAsc() {
    return orderAsc;
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    String result =
        OExecutionStepInternal.getIndent(ctx) + "+ DELETE FROM INDEX " + index.getName();
    if (ctx.isProfilingEnabled()) {
      result += " (" + ctx.getCostFormatted(this) + ")";
    }

    result +=
        (Optional.ofNullable(condition)
            .map(
                oBooleanExpression ->
                    ("\n" + OExecutionStepInternal.getIndent(ctx) + "  " + oBooleanExpression))
            .orElse(""));
    return result;
  }
}
