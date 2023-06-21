package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Created by luigidellaquila on 21/02/17. */
public class FetchEdgesToVerticesStep extends AbstractExecutionStep {
  private final String toAlias;
  private final OIdentifier targetCluster;
  private final OIdentifier targetClass;

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
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx));
    Stream<Object> source = init();

    return new OSubResultsExecutionStream(source.map(this::edges).iterator());
  }

  private Stream<Object> init() {
    Object toValues = null;

    toValues = ctx.getVariable(toAlias);
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    return StreamSupport.stream(Spliterators.spliteratorUnknownSize((Iterator) toValues, 0), false);
  }

  private OExecutionStream edges(Object from) {
    if (from instanceof OResult) {
      from = ((OResult) from).toElement();
    }
    if (from instanceof OIdentifiable && !(from instanceof OElement)) {
      from = ((OIdentifiable) from).getRecord();
    }
    if (from instanceof OElement && ((OElement) from).isVertex()) {
      Iterable<OEdge> edges = ((OElement) from).asVertex().get().getEdges(ODirection.IN);
      Stream<OResult> stream =
          StreamSupport.stream(edges.spliterator(), false)
              .filter(
                  (edge) -> {
                    return matchesClass(edge) && matchesCluster(edge);
                  })
              .map((e) -> new OResultInternal(e));
      return OExecutionStream.resultIterator(stream.iterator());
    } else {
      throw new OCommandExecutionException("Invalid vertex: " + from);
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
