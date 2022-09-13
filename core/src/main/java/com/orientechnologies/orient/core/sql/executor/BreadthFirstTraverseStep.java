package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Created by luigidellaquila on 26/10/16. */
public class BreadthFirstTraverseStep extends AbstractTraverseStep {

  public BreadthFirstTraverseStep(
      List<OTraverseProjectionItem> projections,
      OWhereClause whileClause,
      OInteger maxDepth,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(projections, whileClause, maxDepth, ctx, profilingEnabled);
  }

  @Override
  protected void fetchNextEntryPoints(OCommandContext ctx, int nRecords) {
    OResultSet nextN = getPrev().get().syncPull(ctx, nRecords);
    while (nextN.hasNext()) {
      while (nextN.hasNext()) {
        OResult item = toTraverseResult(nextN.next());
        if (item != null) {
          List<ORID> stack = new ArrayList<>();
          item.getIdentity().ifPresent(x -> stack.add(x));
          ((OResultInternal) item).setMetadata("$stack", stack);

          List<OIdentifiable> path = new ArrayList<>();
          path.add(item.getIdentity().get());
          ((OResultInternal) item).setMetadata("$path", path);

          if (item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
            tryAddEntryPoint(item, ctx);
          }
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
      res = new OTraverseResult(item.getElement().get());
      res.depth = 0;
      res.setMetadata("$depth", 0);
    } else if (item.getPropertyNames().size() == 1) {
      Object val = item.getProperty(item.getPropertyNames().iterator().next());
      if (val instanceof OIdentifiable) {
        res = new OTraverseResult((OIdentifiable) val);
        res.depth = 0;
        res.setMetadata("$depth", 0);
      }
    } else {
      res = new OTraverseResult();
      for (String key : item.getPropertyNames()) {
        res.setProperty(key, item.getProperty(key));
      }
      for (String md : item.getMetadataKeys()) {
        res.setMetadata(md, item.getMetadata(md));
      }
    }

    return res;
  }

  @Override
  protected void fetchNextResults(OCommandContext ctx, int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      OTraverseResult item = (OTraverseResult) this.entryPoints.remove(0);
      this.results.add(item);
      for (OTraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > item.depth) {
          addNextEntryPoints(
              nextStep, item.depth + 1, (List<OIdentifiable>) item.getMetadata("$path"), ctx);
        }
      }
    }
  }

  private void addNextEntryPoints(
      Object nextStep, int depth, List<OIdentifiable> path, OCommandContext ctx) {
    if (nextStep instanceof OIdentifiable) {
      addNextEntryPoints(((OIdentifiable) nextStep), depth, path, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, path, ctx);
    } else if (nextStep instanceof Map) {
      addNextEntryPoints(((Map) nextStep).values().iterator(), depth, path, ctx);
    } else if (nextStep instanceof OResult) {
      addNextEntryPoints(((OResult) nextStep), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(
      Iterator nextStep, int depth, List<OIdentifiable> path, OCommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(
      OIdentifiable nextStep, int depth, List<OIdentifiable> path, OCommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    OTraverseResult res = new OTraverseResult(nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<OIdentifiable> newPath = new ArrayList<>();
    newPath.addAll(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List reverseStack = new ArrayList();
    reverseStack.addAll(newPath);
    Collections.reverse(reverseStack);
    List newStack = new ArrayList();
    newStack.addAll(reverseStack);
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx);
  }

  private void addNextEntryPoints(
      OResult nextStep, int depth, List<OIdentifiable> path, OCommandContext ctx) {
    if (!nextStep.isElement()) {
      return;
    }
    if (this.traversed.contains(nextStep.getElement().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof OTraverseResult) {
      ((OTraverseResult) nextStep).depth = depth;
      ((OTraverseResult) nextStep).setMetadata("$depth", depth);

      List<OIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(((OTraverseResult) nextStep).getIdentity().get());
      ((OTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      List newStack = new ArrayList();
      newStack.addAll(reverseStack);
      ((OTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, ctx);
    } else {
      OTraverseResult res = new OTraverseResult(nextStep.getElement().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);

      List<OIdentifiable> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(((OTraverseResult) nextStep).getIdentity().get());
      ((OTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((OTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(res);
    }
    traversed.add(res.getElement().get().getIdentity());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BREADTH-FIRST TRAVERSE \n");
    if (whileClause != null) {
      result.append(spaces);
      result.append("WHILE " + whileClause.toString());
    }
    return result.toString();
  }
}
