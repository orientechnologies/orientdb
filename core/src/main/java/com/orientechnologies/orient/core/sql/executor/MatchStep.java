package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;
import java.util.Map;
import java.util.Optional;

/** @author Luigi Dell'Aquila */
public class MatchStep extends AbstractExecutionStep {
  protected final EdgeTraversal edge;

  private OResultSet upstream;
  private OResult lastUpstreamRecord;
  private MatchEdgeTraverser traverser;
  private OResult nextResult;

  public MatchStep(OCommandContext context, EdgeTraversal edge, boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.edge = edge;
  }

  @Override
  public void reset() {
    this.upstream = null;
    this.lastUpstreamRecord = null;
    this.traverser = null;
    this.nextResult = null;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    return new OResultSet() {
      private int localCount = 0;

      @Override
      public boolean hasNext() {
        if (localCount >= nRecords) {
          return false;
        }
        if (nextResult == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextResult == null) {
          return false;
        }
        return true;
      }

      @Override
      public OResult next() {
        if (localCount >= nRecords) {
          throw new IllegalStateException();
        }
        if (nextResult == null) {
          fetchNext(ctx, nRecords);
        }
        if (nextResult == null) {
          throw new IllegalStateException();
        }
        OResult result = nextResult;
        fetchNext(ctx, nRecords);
        localCount++;
        ctx.setVariable("$matched", result);
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

  private void fetchNext(OCommandContext ctx, int nRecords) {
    nextResult = null;
    while (true) {
      if (traverser != null && traverser.hasNext(ctx)) {
        nextResult = traverser.next(ctx);
        break;
      }

      if (upstream == null || !upstream.hasNext()) {
        upstream = getPrev().get().syncPull(ctx, nRecords);
      }
      if (!upstream.hasNext()) {
        return;
      }

      lastUpstreamRecord = upstream.next();

      traverser = createTraverser(lastUpstreamRecord);

      boolean found = false;
      while (traverser.hasNext(ctx)) {
        nextResult = traverser.next(ctx);
        if (nextResult != null) {
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }
  }

  protected MatchEdgeTraverser createTraverser(OResult lastUpstreamRecord) {
    if (edge.edge.item instanceof OMultiMatchPathItem) {
      return new MatchMultiEdgeTraverser(lastUpstreamRecord, edge);
    } else if (edge.edge.item instanceof OFieldMatchPathItem) {
      return new MatchFieldTraverser(lastUpstreamRecord, edge);
    } else if (edge.out) {
      return new MatchEdgeTraverser(lastUpstreamRecord, edge);
    } else {
      return new MatchReverseEdgeTraverser(lastUpstreamRecord, edge);
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ MATCH ");
    if (edge.out) {
      result.append("     ---->\n");
    } else {
      result.append("     <----\n");
    }
    result.append(spaces);
    result.append("  ");
    result.append("{" + edge.edge.out.alias + "}");
    if (edge.edge.item instanceof OFieldMatchPathItem) {
      result.append(".");
      result.append(((OFieldMatchPathItem) edge.edge.item).getField());
    } else {
      result.append(edge.edge.item.getMethod());
    }
    result.append("{" + edge.edge.in.alias + "}");
    return result.toString();
  }
}
