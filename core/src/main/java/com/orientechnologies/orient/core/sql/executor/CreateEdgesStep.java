package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OBatch;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Created by luigidellaquila on 28/11/16. */
public class CreateEdgesStep extends AbstractExecutionStep {

  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final String uniqueIndexName;
  private final OIdentifier fromAlias;
  private final OIdentifier toAlias;
  private final Number wait;
  private final Number retry;
  private final OBatch batch;

  public CreateEdgesStep(
      OIdentifier targetClass,
      OIdentifier targetClusterName,
      String uniqueIndex,
      OIdentifier fromAlias,
      OIdentifier toAlias,
      Number wait,
      Number retry,
      OBatch batch,
      OCommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.targetClass = targetClass;
    this.targetCluster = targetClusterName;
    this.uniqueIndexName = uniqueIndex;
    this.fromAlias = fromAlias;
    this.toAlias = toAlias;
    this.wait = wait;
    this.retry = retry;
    this.batch = batch;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    getPrev().ifPresent(x -> x.start(ctx).close(ctx));

    Iterator fromIter = fetchFroms();
    List<Object> toList = fetchTo();
    OIndex uniqueIndex = findIndex(this.uniqueIndexName);
    Stream<OResult> stream =
        StreamSupport.stream(Spliterators.spliteratorUnknownSize(fromIter, 0), false)
            .map(this::asVertex)
            .flatMap(
                (currentFrom) -> {
                  return mapTo(toList, (OVertex) currentFrom, uniqueIndex);
                });
    return OExecutionStream.resultIterator(stream.iterator());
  }

  private OIndex findIndex(String uniqueIndexName) {
    if (uniqueIndexName != null) {
      final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
      OIndex uniqueIndex =
          database.getMetadata().getIndexManagerInternal().getIndex(database, uniqueIndexName);
      if (uniqueIndex == null) {
        throw new OCommandExecutionException("Index not found for upsert: " + uniqueIndexName);
      }
      return uniqueIndex;
    }
    return null;
  }

  private List<Object> fetchTo() {
    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }
    if (toValues instanceof OInternalResultSet) {
      toValues = ((OInternalResultSet) toValues).copy();
    }

    Iterator toIter = (Iterator) toValues;

    if (toIter instanceof OResultSet) {
      try {
        ((OResultSet) toIter).reset();
      } catch (Exception ignore) {
      }
    }
    List<Object> toList = new ArrayList<>();
    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }
    return toList;
  }

  private Iterator fetchFroms() {
    Object fromValues = ctx.getVariable(fromAlias.getStringValue());
    if (fromValues instanceof Iterable && !(fromValues instanceof OIdentifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    if (fromValues instanceof OInternalResultSet) {
      fromValues = ((OInternalResultSet) fromValues).copy();
    }
    Iterator fromIter = (Iterator) fromValues;
    if (fromIter instanceof OResultSet) {
      try {
        ((OResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }
    return fromIter;
  }

  public Stream<OResult> mapTo(List<Object> to, OVertex currentFrom, OIndex uniqueIndex) {
    return to.stream()
        .map(
            (obj) -> {
              OVertex currentTo = asVertex(obj);
              if (currentTo == null) {
                throw new OCommandExecutionException("Invalid TO vertex for edge");
              }
              OEdge edgeToUpdate = null;
              if (uniqueIndex != null) {
                OEdge existingEdge = getExistingEdge(currentFrom, currentTo, uniqueIndex);
                if (existingEdge != null) {
                  edgeToUpdate = existingEdge;
                }
              }
              if (edgeToUpdate == null) {
                edgeToUpdate = currentFrom.addEdge(currentTo, targetClass.getStringValue());
              }
              return new OUpdatableResult(edgeToUpdate);
            });
  }

  private OEdge getExistingEdge(OVertex currentFrom, OVertex currentTo, OIndex uniqueIndex) {
    Object key =
        uniqueIndex.getDefinition().createValue(currentFrom.getIdentity(), currentTo.getIdentity());

    final Iterator<ORID> iterator;
    try (Stream<ORID> stream = uniqueIndex.getInternal().getRids(key)) {
      iterator = stream.iterator();
      if (iterator.hasNext()) {
        return iterator.next().getRecord();
      }
    }

    return null;
  }

  private OVertex asVertex(Object currentFrom) {
    if (currentFrom instanceof ORID) {
      currentFrom = ((ORID) currentFrom).getRecord();
    }
    if (currentFrom instanceof OResult) {
      Object from = currentFrom;
      currentFrom =
          ((OResult) currentFrom)
              .getVertex()
              .orElseThrow(
                  () ->
                      new OCommandExecutionException(
                          "Invalid vertex for edge creation: " + from.toString()));
    }
    if (currentFrom instanceof OVertex) {
      return (OVertex) currentFrom;
    }
    if (currentFrom instanceof OElement) {
      Object from = currentFrom;
      return ((OElement) currentFrom)
          .asVertex()
          .orElseThrow(
              () ->
                  new OCommandExecutionException(
                      "Invalid vertex for edge creation: " + from.toString()));
    }
    throw new OCommandExecutionException(
        "Invalid vertex for edge creation: "
            + (currentFrom == null ? "null" : currentFrom.toString()));
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       CREATE EDGE " + targetClass + " FROM x TO y";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
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
    return new CreateEdgesStep(
        targetClass == null ? null : targetClass.copy(),
        targetCluster == null ? null : targetCluster.copy(),
        uniqueIndexName,
        fromAlias == null ? null : fromAlias.copy(),
        toAlias == null ? null : toAlias.copy(),
        wait,
        retry,
        batch == null ? null : batch.copy(),
        ctx,
        profilingEnabled);
  }
}
