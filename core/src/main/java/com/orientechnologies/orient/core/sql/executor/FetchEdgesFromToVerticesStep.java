package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Created by luigidellaquila on 21/02/17. */
public class FetchEdgesFromToVerticesStep extends AbstractExecutionStep {
  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final String fromAlias;
  private final String toAlias;

  // operation stuff

  // iterator of FROM vertices
  private Iterator fromIter;
  // iterator of edges on current from
  private Iterator<OEdge> currentFromEdgesIter;
  private Iterator toIterator;

  private Set<ORID> toList = new HashSet<>();
  private boolean inited = false;

  private OEdge nextEdge = null;

  public FetchEdgesFromToVerticesStep(
      String fromAlias,
      String toAlias,
      OIdentifier targetClass,
      OIdentifier targetCluster,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    return new OResultSet() {
      private int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && nextEdge != null);
      }

      @Override
      public OResult next() {
        if (!hasNext()) {
          throw new IllegalStateException();
        }
        OEdge edge = nextEdge;
        fetchNextEdge();
        OResultInternal result = new OResultInternal(edge);
        return result;
      }

      @Override
      public void close() {
        if (fromIter instanceof OResultSet) {
          ((OResultSet) fromIter).close();
        }
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

  private OVertex asVertex(Object currentFrom) {
    if (currentFrom instanceof ORID) {
      currentFrom = ((ORID) currentFrom).getRecord();
    }
    if (currentFrom instanceof OResult) {
      return ((OResult) currentFrom).getVertex().orElse(null);
    }
    if (currentFrom instanceof OVertex) {
      return (OVertex) currentFrom;
    }
    if (currentFrom instanceof OElement) {
      return ((OElement) currentFrom).asVertex().orElse(null);
    }
    return null;
  }

  private void init() {
    synchronized (this) {
      if (this.inited) {
        return;
      }
      inited = true;
    }

    Object fromValues = null;

    fromValues = ctx.getVariable(fromAlias);
    if (fromValues instanceof Iterable && !(fromValues instanceof OIdentifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }

    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    fromIter = (Iterator) fromValues;

    Iterator toIter = (Iterator) toValues;

    while (toIter != null && toIter.hasNext()) {
      Object elem = toIter.next();
      if (elem instanceof OResult) {
        elem = ((OResult) elem).toElement();
      }
      if (elem instanceof OIdentifiable && !(elem instanceof OElement)) {
        elem = ((OIdentifiable) elem).getRecord();
      }
      if (!(elem instanceof OElement)) {
        throw new OCommandExecutionException("Invalid vertex: " + elem);
      }
      ((OElement) elem).asVertex().ifPresent(x -> toList.add(x.getIdentity()));
    }

    toIterator = toList.iterator();

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentFromEdgesIter == null || !this.currentFromEdgesIter.hasNext()) {
        if (this.fromIter == null) {
          return;
        }
        if (this.fromIter.hasNext()) {
          Object from = fromIter.next();
          if (from instanceof OResult) {
            from = ((OResult) from).toElement();
          }
          if (from instanceof OIdentifiable && !(from instanceof OElement)) {
            from = ((OIdentifiable) from).getRecord();
          }
          if (from instanceof OElement && ((OElement) from).isVertex()) {
            Iterable<OEdge> edges = ((OElement) from).asVertex().get().getEdges(ODirection.OUT);
            currentFromEdgesIter = edges.iterator();
          } else {
            throw new OCommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      OEdge edge = this.currentFromEdgesIter.next();
      if (toList == null || toList.contains(edge.getTo().getIdentity())) {
        if (matchesClass(edge) && matchesCluster(edge)) {
          this.nextEdge = edge;
          return;
        }
      }
    }
  }

  private boolean matchesCluster(OEdge edge) {
    if (targetCluster == null) {
      return true;
    }
    int clusterId = edge.getIdentity().getClusterId();
    String clusterName = ctx.getDatabase().getClusterNameById(clusterId);
    return clusterName.equals(targetCluster.getStringValue());
  }

  private boolean matchesClass(OEdge edge) {
    if (targetClass == null) {
      return true;
    }
    return edge.getSchemaType().get().isSubClassOf(targetClass.getStringValue());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES FROM x TO y";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchEdgesFromToVerticesStep(
        fromAlias, toAlias, targetClass, targetCluster, ctx, profilingEnabled);
  }
}
