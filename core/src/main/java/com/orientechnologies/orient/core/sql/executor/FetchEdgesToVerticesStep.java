package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/** Created by luigidellaquila on 21/02/17. */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {
  private final String toAlias;
  private final OIdentifier targetCluster;
  private final OIdentifier targetClass;

  private boolean inited = false;
  private Iterator toIter;
  private OEdge nextEdge;
  private Iterator<OEdge> currentToEdgesIter;

  public FetchEdgesToVerticesStep(
      String toAlias,
      OIdentifier targetClass,
      OIdentifier targetCluster,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.toAlias = toAlias;
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
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
        if (toIter instanceof OResultSet) {
          ((OResultSet) toIter).close();
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

  private void init() {
    synchronized (this) {
      if (this.inited) {
        return;
      }
      inited = true;
    }

    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    this.toIter = (Iterator) toValues;

    fetchNextEdge();
  }

  private void fetchNextEdge() {
    this.nextEdge = null;
    while (true) {
      while (this.currentToEdgesIter == null || !this.currentToEdgesIter.hasNext()) {
        if (this.toIter == null) {
          return;
        }
        if (this.toIter.hasNext()) {
          Object from = toIter.next();
          if (from instanceof OResult) {
            from = ((OResult) from).toElement();
          }
          if (from instanceof OIdentifiable && !(from instanceof OElement)) {
            from = ((OIdentifiable) from).getRecord();
          }
          if (from instanceof OElement && ((OElement) from).isVertex()) {
            Iterable<OEdge> edges = ((OElement) from).asVertex().get().getEdges(ODirection.IN);
            currentToEdgesIter = edges.iterator();
          } else {
            throw new OCommandExecutionException("Invalid vertex: " + from);
          }
        } else {
          return;
        }
      }
      OEdge edge = this.currentToEdgesIter.next();
      if (matchesClass(edge) && matchesCluster(edge)) {
        this.nextEdge = edge;
        return;
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
    String result = spaces + "+ FOR EACH x in " + toAlias + "\n";
    result += spaces + "       FETCH EDGES TO x";
    if (targetClass != null) {
      result += "\n" + spaces + "       (target class " + targetClass + ")";
    }
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }
}
