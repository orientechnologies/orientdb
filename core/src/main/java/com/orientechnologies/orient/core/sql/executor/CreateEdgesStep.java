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
import com.orientechnologies.orient.core.sql.parser.OBatch;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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

  // operation stuff
  private Iterator fromIter;
  private Iterator toIterator;
  private OVertex currentFrom;
  private OVertex currentTo;
  private OEdge edgeToUpdate; // for upsert
  private boolean finished = false;
  private List toList = new ArrayList<>();
  private OIndex uniqueIndex;

  private boolean inited = false;

  private long cost = 0;

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
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    init();
    return new OResultSet() {
      private int currentBatch = 0;

      @Override
      public boolean hasNext() {
        if (currentTo == null) {
          loadNextFromTo();
        }
        return (currentBatch < nRecords && currentTo != null && !finished);
      }

      @Override
      public OResult next() {
        if (currentTo == null) {
          loadNextFromTo();
        }
        long begin = profilingEnabled ? System.nanoTime() : 0;
        try {

          if (finished || currentBatch >= nRecords) {
            throw new IllegalStateException();
          }
          if (currentTo == null) {
            throw new OCommandExecutionException("Invalid TO vertex for edge");
          }

          OEdge edge =
              edgeToUpdate != null
                  ? edgeToUpdate
                  : currentFrom.addEdge(currentTo, targetClass.getStringValue());

          OUpdatableResult result = new OUpdatableResult(edge);
          currentTo = null;
          currentBatch++;
          return result;
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

      @Override
      public void close() {}

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
    Object fromValues = ctx.getVariable(fromAlias.getStringValue());
    if (fromValues instanceof Iterable && !(fromValues instanceof OIdentifiable)) {
      fromValues = ((Iterable) fromValues).iterator();
    } else if (!(fromValues instanceof Iterator)) {
      fromValues = Collections.singleton(fromValues).iterator();
    }
    if (fromValues instanceof OInternalResultSet) {
      fromValues = ((OInternalResultSet) fromValues).copy();
    }

    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }
    if (toValues instanceof OInternalResultSet) {
      toValues = ((OInternalResultSet) toValues).copy();
    }

    fromIter = (Iterator) fromValues;
    if (fromIter instanceof OResultSet) {
      try {
        ((OResultSet) fromIter).reset();
      } catch (Exception ignore) {
      }
    }

    Iterator toIter = (Iterator) toValues;

    if (toIter instanceof OResultSet) {
      try {
        ((OResultSet) toIter).reset();
      } catch (Exception ignore) {
      }
    }
    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }

    toIterator = toList.iterator();

    currentFrom = fromIter != null && fromIter.hasNext() ? asVertex(fromIter.next()) : null;

    if (uniqueIndexName != null) {
      final ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) ctx.getDatabase();
      uniqueIndex =
          database.getMetadata().getIndexManagerInternal().getIndex(database, uniqueIndexName);
      if (uniqueIndex == null) {
        throw new OCommandExecutionException("Index not found for upsert: " + uniqueIndexName);
      }
    }
  }

  protected void loadNextFromTo() {
    long begin = profilingEnabled ? System.nanoTime() : 0;
    try {
      edgeToUpdate = null;
      this.currentTo = null;
      if (!toIterator.hasNext()) {
        toIterator = toList.iterator();
        if (!fromIter.hasNext()) {
          finished = true;
          return;
        }
        currentFrom = fromIter.hasNext() ? asVertex(fromIter.next()) : null;
      }
      if (toIterator.hasNext() || (toList.size() > 0 && fromIter.hasNext())) {
        if (currentFrom == null) {
          if (!fromIter.hasNext()) {
            finished = true;
            return;
          }
        }

        Object obj = toIterator.next();

        currentTo = asVertex(obj);
        if (currentTo == null) {
          throw new OCommandExecutionException("Invalid TO vertex for edge");
        }

        if (isUpsert()) {
          OEdge existingEdge = getExistingEdge(currentFrom, currentTo);
          if (existingEdge != null) {
            edgeToUpdate = existingEdge;
          }
        }
        return;

      } else {
        this.currentTo = null;
        return;
      }
    } finally {
      if (profilingEnabled) {
        cost += (System.nanoTime() - begin);
      }
    }
  }

  private OEdge getExistingEdge(OVertex currentFrom, OVertex currentTo) {
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

  private boolean isUpsert() {
    return uniqueIndex != null;
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
  public long getCost() {
    return cost;
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
