package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public class DepthFirstTraverseStep extends AbstractTraverseStep {

  public DepthFirstTraverseStep(List<OTraverseProjectionItem> projections, OWhereClause whileClause, OCommandContext ctx) {
    super(projections, whileClause, ctx);
  }

  @Override protected void fetchNextEntryPoints(OCommandContext ctx, int nRecords) {
    OTodoResultSet nextN = getPrev().get().syncPull(ctx, nRecords);
    while (nextN.hasNext()) {
      OResult item = toTraverseResult(nextN.next());
      if (item != null && item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
        tryAddEntryPoint(item, ctx);
        traversed.add(item.getElement().get().getIdentity());
      }
    }
  }

  private OResult toTraverseResult(OResult item) {
    OTraverseResult res = null;
    if (item instanceof OTraverseResult) {
      res = (OTraverseResult) item;
    } else if (item.isElement() && item.getElement().get().getIdentity().isPersistent()) {
      res = new OTraverseResult();
      res.setElement(item.getElement().get());
      res.depth = 0;
    } else {
      return null;
    }

    return res;
  }

  @Override protected void fetchNextResults(OCommandContext ctx, int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      OTraverseResult item = (OTraverseResult) this.entryPoints.remove(0);
      this.results.add(item);
      for (OTraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        addNextEntryPoints(nextStep, item.depth + 1, ctx);
      }
    }
  }

  private void addNextEntryPoints(Object nextStep, int depth, OCommandContext ctx) {
    if (nextStep instanceof OIdentifiable) {
      addNextEntryPoint(((OIdentifiable) nextStep), depth, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, ctx);
    } else if (nextStep instanceof OResult) {
      addNextEntryPoint(((OResult) nextStep), depth, ctx);
    }
  }

  private void addNextEntryPoints(Iterator nextStep, int depth, OCommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, ctx);
    }
  }

  private void addNextEntryPoint(OIdentifiable nextStep, int depth, OCommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    OTraverseResult res = new OTraverseResult();
    res.setElement(nextStep);
    res.depth = depth;
    tryAddEntryPoint(res, ctx);
  }

  private void addNextEntryPoint(OResult nextStep, int depth, OCommandContext ctx) {
    if (!nextStep.isElement()) {
      return;
    }
    if (this.traversed.contains(nextStep.getElement().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof OTraverseResult) {
      ((OTraverseResult) nextStep).depth = depth;
      tryAddEntryPoint(nextStep, ctx);
    } else {
      OTraverseResult res = new OTraverseResult();
      res.setElement(nextStep.getElement().get());
      res.depth = depth;
      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(0, res);
    }
    traversed.add(res.getElement().get().getIdentity());
  }

}
