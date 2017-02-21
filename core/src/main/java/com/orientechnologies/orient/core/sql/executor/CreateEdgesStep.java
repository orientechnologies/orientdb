package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.parser.OBatch;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.*;

/**
 * Created by luigidellaquila on 28/11/16.
 */
public class CreateEdgesStep extends AbstractExecutionStep {

  private final OIdentifier targetClass;
  private final OIdentifier targetCluster;
  private final OIdentifier fromAlias;
  private final OIdentifier toAlias;
  private final Number      wait;
  private final Number      retry;
  private final OBatch      batch;

  //operation stuff
  Iterator fromIter;
  Iterator toIterator;
  OVertex  currentFrom;
  List toList = new ArrayList<>();
  private boolean inited = false;

  public CreateEdgesStep(OIdentifier targetClass, OIdentifier targetClusterName, OIdentifier fromAlias, OIdentifier toAlias,
      Number wait, Number retry, OBatch batch, OCommandContext ctx) {
    super(ctx);
    this.targetClass = targetClass;
    this.targetCluster = targetClusterName;
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
      int currentBatch = 0;

      @Override
      public boolean hasNext() {
        return (currentBatch < nRecords && (toIterator.hasNext() || (toList.size() > 0 && fromIter.hasNext())));
      }

      @Override
      public OResult next() {
        if (!toIterator.hasNext()) {
          toIterator = toList.iterator();
          if (!fromIter.hasNext()) {
            throw new IllegalStateException();
          }
          currentFrom = fromIter.hasNext() ? asVertex(fromIter.next()) : null;
        }
        if (currentBatch < nRecords && (toIterator.hasNext() || (toList.size() > 0 && fromIter.hasNext()))) {
          if (currentFrom == null) {
            throw new OCommandExecutionException("Invalid FROM vertex for edge");
          }

          OVertex currentTo = asVertex(toIterator.next());
          if (currentTo == null) {
            throw new OCommandExecutionException("Invalid TO vertex for edge");
          }

          OEdge edge = currentFrom.addEdge(currentTo, targetClass.getStringValue());

          OUpdatableResult result = new OUpdatableResult(edge);
          result.setElement(edge);
          currentBatch++;
          return result;
        } else {
          throw new IllegalStateException();
        }
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
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

    Object toValues = ctx.getVariable(toAlias.getStringValue());
    if (toValues instanceof Iterable && !(toValues instanceof OIdentifiable)) {
      toValues = ((Iterable) toValues).iterator();
    } else if (!(toValues instanceof Iterator)) {
      toValues = Collections.singleton(toValues).iterator();
    }

    fromIter = (Iterator) fromValues;

    Iterator toIter = (Iterator) toValues;

    while (toIter != null && toIter.hasNext()) {
      toList.add(toIter.next());
    }

    toIterator = toList.iterator();

    currentFrom = fromIter != null && fromIter.hasNext() ? asVertex(fromIter.next()) : null;

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

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FOR EACH x in " + fromAlias + "\n";
    result += spaces + "    FOR EACH y in " + toAlias + "\n";
    result += spaces + "       CREATE EDGE " + targetClass + " FROM x TO y";
    if (targetCluster != null) {
      result += "\n" + spaces + "       (target cluster " + targetCluster + ")";
    }
    return result;
  }
}


