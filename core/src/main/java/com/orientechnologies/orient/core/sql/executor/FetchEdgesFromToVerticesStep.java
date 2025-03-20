package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.StreamSupport;

/** Created by luigidellaquila on 21/02/17. */
public class FetchEdgesFromToVerticesStep extends AbstractExecutionStep {
  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final String fromAlias;
  private final String toAlias;

  public FetchEdgesFromToVerticesStep(
      String fromAlias, String toAlias, OIdentifier targetClass, OIdentifier targetCluster) {
    super();
    this.targetClass = targetClass;
    this.targetCluster = targetCluster;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));

    final Iterator fromIter = loadFrom(ctx);

    final Set<ORID> toList = loadTo(ctx);

    return OExecutionStream.streamsFromIterator(
        fromIter, (value, pc) -> createResultSet(toList, value, ctx));
  }

  private OExecutionStream createResultSet(Set<ORID> toList, Object val, OCommandContext ctx) {
    return OExecutionStream.resultIterator(
        StreamSupport.stream(this.loadNextResults(val).spliterator(), false)
            .filter((e) -> filterResult(e, toList, ctx))
            .map(
                (edge) -> {
                  return (OResult) new OResultInternal(edge);
                })
            .iterator());
  }

  private Set<ORID> loadTo(OCommandContext ctx) {
    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator) && toValues != null) {
      toValues = Collections.singleton(toValues).iterator();
    }

    Iterator toIter = (Iterator) toValues;
    if (toIter != null) {
      final Set<ORID> toList = new HashSet<ORID>();
      while (toIter.hasNext()) {
        Object elem = toIter.next();
        if (elem instanceof OResult && ((OResult) elem).isElement()) {
          elem = ((OResult) elem).getElement().get();
        }
        if (elem instanceof OIdentifiable && !(elem instanceof OElement)) {
          elem = ((OIdentifiable) elem).getRecord();
        }
        if (!(elem instanceof OElement)) {
          throw new OCommandExecutionException("Invalid vertex: " + elem);
        }
        ((OElement) elem).asVertex().ifPresent(x -> toList.add(x.getIdentity()));
      }

      return toList;
    }
    return null;
  }

  private Iterator loadFrom(OCommandContext ctx) {
    Object fromValues = null;

    fromValues = ctx.getVariable(fromAlias);
    if (fromValues instanceof Iterable && !(fromValues instanceof OIdentifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    return (Iterator) fromValues;
  }

  private boolean filterResult(OEdge edge, Set<ORID> toList, OCommandContext ctx) {
    if (toList == null || toList.contains(edge.getTo().getIdentity())) {
      if (matchesClass(edge) && matchesCluster(edge, ctx)) {
        return true;
      } else {
        return false;
      }
    }
    return true;
  }

  private Iterable<OEdge> loadNextResults(Object from) {
    if (from instanceof OResult && ((OResult) from).isElement()) {
      from = ((OResult) from).getElement().get();
    }
    if (from instanceof OIdentifiable && !(from instanceof OElement)) {
      from = ((OIdentifiable) from).getRecord();
    }
    if (from instanceof OElement && ((OElement) from).isVertex()) {
      Iterable<OEdge> edges = ((OElement) from).asVertex().get().getEdges(ODirection.OUT);
      return edges;
    } else {
      throw new OCommandExecutionException("Invalid vertex: " + from);
    }
  }

  private boolean matchesCluster(OEdge edge, OCommandContext ctx) {
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
  public String prettyPrint(OPrintContext ctx) {
    String spaces = OExecutionStepInternal.getIndent(ctx);
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
  public OExecutionStepInternal copy(OCommandContext ctx) {
    return new FetchEdgesFromToVerticesStep(fromAlias, toAlias, targetClass, targetCluster);
  }
}
