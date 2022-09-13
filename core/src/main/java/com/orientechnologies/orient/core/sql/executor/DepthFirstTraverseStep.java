package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.sql.parser.OTraverseProjectionItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Created by luigidellaquila on 26/10/16. */
public class DepthFirstTraverseStep extends AbstractTraverseStep {

  public DepthFirstTraverseStep(
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
      OResult item = toTraverseResult(nextN.next());
      if (item == null) {
        continue;
      }
      ((OResultInternal) item).setMetadata("$depth", 0);

      List stack = new ArrayList();
      item.getIdentity().ifPresent(x -> stack.add(x));
      ((OResultInternal) item).setMetadata("$stack", stack);

      List<OIdentifiable> path = new ArrayList<>();
      if (item.getIdentity().isPresent()) {
        path.add(item.getIdentity().get());
      } else if (item.getProperty("@rid") != null) {
        path.add(item.getProperty("@rid"));
      }
      ((OResultInternal) item).setMetadata("$path", path);

      if (item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
        tryAddEntryPointAtTheEnd(item, ctx);
        traversed.add(item.getElement().get().getIdentity());
      } else if (item.getProperty("@rid") != null
          && item.getProperty("@rid") instanceof OIdentifiable) {
        tryAddEntryPointAtTheEnd(item, ctx);
        traversed.add(((OIdentifiable) item.getProperty("@rid")).getIdentity());
      }
    }
  }

  private OResult toTraverseResult(OResult item) {
    OTraverseResult res = null;
    if (item instanceof OTraverseResult) {
      res = (OTraverseResult) item;
    } else if (item.isElement() && item.getElement().get().getIdentity().isValid()) {
      res = new OTraverseResult(item.getElement().get());
      res.depth = 0;
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
        res.setProperty(key, convert(item.getProperty(key)));
      }
      for (String md : item.getMetadataKeys()) {
        res.setMetadata(md, item.getMetadata(md));
      }
    }

    return res;
  }

  public Object convert(Object value) {
    if (value instanceof ORidBag) {
      List result = new ArrayList();
      ((ORidBag) value).forEach(x -> result.add(x));
      return result;
    }
    return value;
  }

  @Override
  protected void fetchNextResults(OCommandContext ctx, int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      OTraverseResult item = (OTraverseResult) this.entryPoints.remove(0);
      this.results.add(item);
      for (OTraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        Integer depth = item.depth != null ? item.depth : (Integer) item.getMetadata("$depth");
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > depth) {
          addNextEntryPoints(
              nextStep,
              depth + 1,
              (List) item.getMetadata("$path"),
              (List) item.getMetadata("$stack"),
              ctx);
        }
      }
    }
  }

  private void addNextEntryPoints(
      Object nextStep,
      int depth,
      List<OIdentifiable> path,
      List<OIdentifiable> stack,
      OCommandContext ctx) {
    if (nextStep instanceof OIdentifiable) {
      addNextEntryPoint(((OIdentifiable) nextStep), depth, path, stack, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, path, stack, ctx);
    } else if (nextStep instanceof Map) {
      addNextEntryPoints(((Map) nextStep).values().iterator(), depth, path, stack, ctx);
    } else if (nextStep instanceof OResult) {
      addNextEntryPoint(((OResult) nextStep), depth, path, stack, ctx);
    }
  }

  private void addNextEntryPoints(
      Iterator nextStep,
      int depth,
      List<OIdentifiable> path,
      List<OIdentifiable> stack,
      OCommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, stack, ctx);
    }
  }

  private void addNextEntryPoint(
      OIdentifiable nextStep,
      int depth,
      List<OIdentifiable> path,
      List<OIdentifiable> stack,
      OCommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    OTraverseResult res = new OTraverseResult(nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<OIdentifiable> newPath = new ArrayList<>(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List newStack = new ArrayList();
    newStack.add(res.getIdentity().get());
    newStack.addAll(stack);
    //    for (int i = 0; i < newPath.size(); i++) {
    //      newStack.offerLast(newPath.get(i));
    //    }
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx);
  }

  private void addNextEntryPoint(
      OResult nextStep,
      int depth,
      List<OIdentifiable> path,
      List<OIdentifiable> stack,
      OCommandContext ctx) {
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
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
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
      nextStep.getIdentity().ifPresent(x -> newPath.add(x.getIdentity()));
      ((OTraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      List newStack = new ArrayList();
      newStack.addAll(reverseStack);
      ((OTraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(0, res);
    }

    if (res.isElement()) {
      traversed.add(res.getElement().get().getIdentity());
    } else if (res.getProperty("@rid") != null
        && res.getProperty("@rid") instanceof OIdentifiable) {
      traversed.add(((OIdentifiable) res.getProperty("@rid")).getIdentity());
    }
  }

  private void tryAddEntryPointAtTheEnd(OResult res, OCommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(res);
    }

    if (res.isElement()) {
      traversed.add(res.getElement().get().getIdentity());
    } else if (res.getProperty("@rid") != null
        && res.getProperty("@rid") instanceof OIdentifiable) {
      traversed.add(((OIdentifiable) res.getProperty("@rid")).getIdentity());
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ DEPTH-FIRST TRAVERSE \n");
    result.append(spaces);
    result.append("  " + projections.toString());
    if (whileClause != null) {
      result.append("\n");
      result.append(spaces);
      result.append("WHILE " + whileClause.toString());
    }
    return result.toString();
  }
}
