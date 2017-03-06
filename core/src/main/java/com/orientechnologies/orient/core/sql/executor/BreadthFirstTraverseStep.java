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
public class BreadthFirstTraverseStep extends AbstractTraverseStep {

  public BreadthFirstTraverseStep(List<OTraverseProjectionItem> projections, OWhereClause whileClause, OCommandContext ctx) {
    super(projections, whileClause, ctx);
  }

  @Override protected void fetchNextEntryPoints(OCommandContext ctx, int nRecords) {
    OResultSet nextN = getPrev().get().syncPull(ctx, nRecords);
    while (nextN.hasNext()) {
      while (nextN.hasNext()) {
        OResult item = toTraverseResult(nextN.next());
        if (item != null && item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
          tryAddEntryPoint(item, ctx);

        }
      }
      nextN = getPrev().get().syncPull(ctx, nRecords);
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
      res.setMetadata("$depth", 0);
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
      addNextEntryPoints(((OIdentifiable) nextStep), depth, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, ctx);
    } else if (nextStep instanceof OResult) {
      addNextEntryPoints(((OResult) nextStep), depth, ctx);
    }
  }

  private void addNextEntryPoints(Iterator nextStep, int depth, OCommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, ctx);
    }
  }

  private void addNextEntryPoints(OIdentifiable nextStep, int depth, OCommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    OTraverseResult res = new OTraverseResult();
    res.setElement(nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);
    tryAddEntryPoint(res, ctx);

  }

  private void addNextEntryPoints(OResult nextStep, int depth, OCommandContext ctx) {
    if (!nextStep.isElement()) {
      return;
    }
    if (this.traversed.contains(nextStep.getElement().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof OTraverseResult) {
      ((OTraverseResult) nextStep).depth = depth;
      ((OTraverseResult) nextStep).setMetadata("$depth", depth);
      tryAddEntryPoint(nextStep, ctx);
    } else {
      OTraverseResult res = new OTraverseResult();
      res.setElement(nextStep.getElement().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);
      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(res);
    }
    traversed.add(res.getElement().get().getIdentity());
  }
}
