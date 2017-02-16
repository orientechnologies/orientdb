package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by luigidellaquila on 14/11/16.
 */
public class OMatchPathItemIterator implements Iterator<OIdentifiable> {

  private final OMatchStatement.MatchContext matchContext;
  private final OCommandContext              ctx;
  OMatchPathItem item;

  OIdentifiable nextElement = null;

  List<Iterator> stack = new LinkedList<Iterator>();

  OMatchPathItemIterator(OMatchPathItem item, OMatchStatement.MatchContext matchContext, OCommandContext iCommandContext,
      final OIdentifiable startingPoint) {
    this.item = item;
    this.matchContext = matchContext;
    this.ctx = iCommandContext;
    this.stack.add(new Iterator() {
      boolean executed = false;

      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public Object next() {
        if (executed) {
          throw new IllegalStateException();
        }
        executed = true;
        return startingPoint;
      }

      @Override
      public void remove() {

      }
    });

    loadNext();
  }

  protected void loadNext() {
    while (stack.size() > 0) {
      if (!stack.get(0).hasNext()) {
        stack.remove(0);
        continue;
      }
      loadNextInternal();
      if (nextElement != null) {
        return;
      }
    }
  }

  protected void loadNextInternal() {
    if (stack == null || stack.size() == 0) {
      nextElement = null;
      return;
    }

    int depth = stack.size() - 1;

    Object oldDepth = ctx.getVariable("$depth", depth);
    ctx.setVariable("$depth", depth);

    final OWhereClause filter = this.item.filter == null ? null : this.item.filter.getFilter();
    final OWhereClause whileCondition = this.item.filter == null ? null : this.item.filter.getWhileCondition();
    final Integer maxDepth = maxDepth(this.item.filter);
    final OClass oClass =
        this.item.filter == null ? null : item.getDatabase().getMetadata().getSchema().getClass(this.item.filter.getClassName(ctx));

    boolean notDeep = this.item.filter == null || (whileCondition == null && this.item.filter.getMaxDepth() == null);

    OIdentifiable startingPoint = (OIdentifiable) stack.get(0).next();
    nextElement = null;
    if (this.item.filter == null || (whileCondition == null && this.item.filter.getMaxDepth() == null)) {
      //basic case, no traversal, discard level zero
      if (depth == 1) {
        Object prevMatch = ctx.getVariable("$currentMatch");
        Object prevCurrent = ctx.getVariable("$current");
        ctx.setVariable("$currentMatch", startingPoint);
        ctx.setVariable("$current", startingPoint);

        if (filter == null || filter.matchesFilters(startingPoint, ctx)) {
          nextElement = startingPoint;
        }
        ctx.setVariable("$current", prevCurrent);
        ctx.setVariable("$currentMatch", prevMatch);
      }
    } else {
      Object prevMatch = ctx.getVariable("$currentMatch");
      ctx.setVariable("$currentMatch", startingPoint);
      if (filter == null || filter.matchesFilters(startingPoint, ctx)) {
        nextElement = startingPoint;
      }
      ctx.setVariable("$currentMatch", prevMatch);
    }

    if ((notDeep && depth == 0) || ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition
        .matchesFilters(startingPoint, ctx)) && (oClass == null || matchesClass(oClass, startingPoint)))) {
      stack.add(0, item.traversePatternEdge(matchContext, startingPoint, ctx).iterator());
    }
    ctx.setVariable("$depth", oldDepth);
  }  @Override
  public boolean hasNext() {
    while (stack.size() > 0 && nextElement == null) {
      loadNext();
    }
    return nextElement != null;
  }

  private Integer maxDepth(OMatchFilter filter) {
    if (filter == null) {
      return 1;
    }
    if (filter.getMaxDepth() != null) {
      return filter.getMaxDepth();
    }
    if (filter.getWhileCondition() == null) {
      return 1;
    }
    return null;
  }

  private boolean matchesClass(OClass oClass, OIdentifiable startingPoint) {
    if (oClass == null) {
      return true;
    }
    ODocument doc = startingPoint.getRecord();
    if (doc == null) {
      return false;
    }
    OClass clazz = doc.getSchemaClass();
    if (clazz == null) {
      return false;
    }
    return clazz.isSubClassOf(oClass);
  }  @Override
  public OIdentifiable next() {
    if (nextElement == null) {
      throw new IllegalStateException();
    }
    OIdentifiable result = nextElement;
    nextElement = null;
    while (stack.size() > 0 && nextElement == null) {
      loadNext();
    }
    return result;
  }



  @Override
  public void remove() {

  }



}