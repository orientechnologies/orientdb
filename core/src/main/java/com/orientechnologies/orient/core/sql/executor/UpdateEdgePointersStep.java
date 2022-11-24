package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * after an update of an edge, this step updates edge pointers on vertices to make the graph
 * consistent again
 */
public class UpdateEdgePointersStep extends AbstractExecutionStep {

  public UpdateEdgePointersStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    OResultSet upstream = getPrev().get().syncPull(ctx, nRecords);
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return upstream.hasNext();
      }

      @Override
      public OResult next() {
        OResult result = upstream.next();
        if (result instanceof OResultInternal) {
          handleUpdateEdge((ODocument) result.getElement().get().getRecord());
        }
        return result;
      }

      private void updateIn(OResult item) {}

      private void updateOut(OResult item) {}

      @Override
      public void close() {
        upstream.close();
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

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE EDGE POINTERS");
    return result.toString();
  }

  /**
   * handles vertex consistency after an UPDATE EDGE
   *
   * @param record the edge record
   */
  private void handleUpdateEdge(ODocument record) {
    Object currentOut = record.field("out");
    Object currentIn = record.field("in");

    Object prevOut = record.getOriginalValue("out");
    Object prevIn = record.getOriginalValue("in");

    // to manage subqueries
    if (currentOut instanceof Collection && ((Collection) currentOut).size() == 1) {
      currentOut = ((Collection) currentOut).iterator().next();
      record.setProperty("out", currentOut);
    }
    if (currentIn instanceof Collection && ((Collection) currentIn).size() == 1) {
      currentIn = ((Collection) currentIn).iterator().next();
      record.setProperty("in", currentIn);
    }

    validateOutInForEdge(record, currentOut, currentIn);

    changeVertexEdgePointer(record, (OIdentifiable) prevIn, (OIdentifiable) currentIn, "in");
    changeVertexEdgePointer(record, (OIdentifiable) prevOut, (OIdentifiable) currentOut, "out");
  }

  /**
   * updates old and new vertices connected to an edge after out/in update on the edge itself
   *
   * @param edge the edge
   * @param prevVertex the previously connected vertex
   * @param currentVertex the currently connected vertex
   * @param direction the direction ("out" or "in")
   */
  private void changeVertexEdgePointer(
      ODocument edge, OIdentifiable prevVertex, OIdentifiable currentVertex, String direction) {
    if (prevVertex != null && !prevVertex.equals(currentVertex)) {
      String edgeClassName = edge.getClassName();
      if (edgeClassName.equalsIgnoreCase("E")) {
        edgeClassName = "";
      }
      String vertexFieldName = direction + "_" + edgeClassName;
      ODocument prevOutDoc = ((OIdentifiable) prevVertex).getRecord();
      ORidBag prevBag = prevOutDoc.field(vertexFieldName);
      if (prevBag != null) {
        prevBag.remove(edge);
        prevOutDoc.save();
      }

      ODocument currentVertexDoc = ((OIdentifiable) currentVertex).getRecord();
      ORidBag currentBag = currentVertexDoc.field(vertexFieldName);
      if (currentBag == null) {
        currentBag = new ORidBag();
        currentVertexDoc.field(vertexFieldName, currentBag);
      }
      currentBag.add(edge);
    }
  }

  private void validateOutInForEdge(ODocument record, Object currentOut, Object currentIn) {
    if (!isRecordInstanceOf(currentOut, "V")) {
      throw new OCommandExecutionException(
          "Error updating edge: 'out' is not a vertex - " + currentOut + "");
    }
    if (!isRecordInstanceOf(currentIn, "V")) {
      throw new OCommandExecutionException(
          "Error updating edge: 'in' is not a vertex - " + currentIn + "");
    }
  }

  /**
   * checks if an object is an OIdentifiable and an instance of a particular (schema) class
   *
   * @param iRecord The record object
   * @param orientClass The schema class
   * @return
   */
  private boolean isRecordInstanceOf(Object iRecord, String orientClass) {
    if (iRecord == null) {
      return false;
    }
    if (!(iRecord instanceof OIdentifiable)) {
      return false;
    }
    ODocument record = ((OIdentifiable) iRecord).getRecord();
    if (iRecord == null) {
      return false;
    }
    return (ODocumentInternal.getImmutableSchemaClass(record).isSubClassOf(orientClass));
  }
}
