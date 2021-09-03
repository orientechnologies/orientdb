package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OStorageRecoverEventListener;
import com.tinkerpop.blueprints.Direction;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Repairs a graph. Current implementation scan the entire graph. In the future the WAL will be used
 * to make this repair task much faster.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OGraphRepair {

  private class ORepairStats {
    protected long scannedEdges = 0;
    protected long removedEdges = 0;
    protected long scannedVertices = 0;
    protected long scannedLinks = 0;
    protected long removedLinks = 0;
    protected long repairedVertices = 0;
  }

  private OStorageRecoverEventListener eventListener;

  public void repair(
      final OrientBaseGraph graph,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(
        outputListener, "Repair of graph '" + graph.getRawGraph().getURL() + "' is started ...\n");

    final long beginTime = System.currentTimeMillis();

    final ORepairStats stats = new ORepairStats();

    // SCAN AND CLEAN ALL THE EDGES FIRST (IF ANY)
    repairEdges(graph, stats, outputListener, options, false);

    // SCAN ALL THE VERTICES
    repairVertices(graph, stats, outputListener, options, false);

    message(
        outputListener,
        "Repair of graph '"
            + graph.getRawGraph().getURL()
            + "' completed in "
            + ((System.currentTimeMillis() - beginTime) / 1000)
            + " secs\n");

    message(outputListener, " scannedEdges.....: " + stats.scannedEdges + "\n");
    message(outputListener, " removedEdges.....: " + stats.removedEdges + "\n");
    message(outputListener, " scannedVertices..: " + stats.scannedVertices + "\n");
    message(outputListener, " scannedLinks.....: " + stats.scannedLinks + "\n");
    message(outputListener, " removedLinks.....: " + stats.removedLinks + "\n");
    message(outputListener, " repairedVertices.: " + stats.repairedVertices + "\n");
  }

  public void check(
      final OrientBaseGraph graph,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options) {
    message(
        outputListener, "Check of graph '" + graph.getRawGraph().getURL() + "' is started...\n");

    final long beginTime = System.currentTimeMillis();

    final ORepairStats stats = new ORepairStats();

    // SCAN AND CLEAN ALL THE EDGES FIRST (IF ANY)
    repairEdges(graph, stats, outputListener, options, true);

    // SCAN ALL THE VERTICES
    repairVertices(graph, stats, outputListener, options, true);

    message(
        outputListener,
        "Check of graph '"
            + graph.getRawGraph().getURL()
            + "' completed in "
            + ((System.currentTimeMillis() - beginTime) / 1000)
            + " secs\n");

    message(outputListener, " scannedEdges.....: " + stats.scannedEdges + "\n");
    message(outputListener, " edgesToRemove....: " + stats.removedEdges + "\n");
    message(outputListener, " scannedVertices..: " + stats.scannedVertices + "\n");
    message(outputListener, " scannedLinks.....: " + stats.scannedLinks + "\n");
    message(outputListener, " linksToRemove....: " + stats.removedLinks + "\n");
    message(outputListener, " verticesToRepair.: " + stats.repairedVertices + "\n");
  }

  protected void repairEdges(
      final OrientBaseGraph graph,
      final ORepairStats stats,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final ODatabaseDocument db = graph.getRawGraph();
    final OMetadata metadata = db.getMetadata();
    final OSchema schema = metadata.getSchema();
    final OrientConfigurableGraph.Settings settings = graph.settings;

    final boolean useVertexFieldsForEdgeLabels = settings.isUseVertexFieldsForEdgeLabels();

    final OClass edgeClass = schema.getClass(OrientEdgeType.CLASS_NAME);
    if (edgeClass != null) {
      final long countEdges = db.countClass(edgeClass.getName());

      long skipEdges = 0l;
      if (options != null && options.get("-skipEdges") != null) {
        skipEdges = Long.parseLong(options.get("-skipEdges").get(0));
      }

      message(
          outputListener, "Scanning " + countEdges + " edges (skipEdges=" + skipEdges + ")...\n");

      long parsedEdges = 0l;
      final long beginTime = System.currentTimeMillis();

      for (ODocument edge : db.browseClass(edgeClass.getName())) {
        final ORID edgeId = edge.getIdentity();

        parsedEdges++;
        if (skipEdges > 0 && parsedEdges <= skipEdges) continue;

        stats.scannedEdges++;

        if (eventListener != null) eventListener.onScannedEdge(edge);

        if (outputListener != null && stats.scannedEdges % 100000 == 0) {
          long speedPerSecond =
              (long) (parsedEdges / ((System.currentTimeMillis() - beginTime) / 1000.0));
          if (speedPerSecond < 1) speedPerSecond = 1;
          final long remaining = (countEdges - parsedEdges) / speedPerSecond;

          message(
              outputListener,
              "+ edges: scanned "
                  + stats.scannedEdges
                  + ", removed "
                  + stats.removedEdges
                  + " (estimated remaining time "
                  + remaining
                  + " secs)\n");
        }

        boolean outVertexMissing = false;

        String removalReason = "";

        final OIdentifiable out = OrientEdge.getConnection(edge, Direction.OUT);
        if (out == null) outVertexMissing = true;
        else {
          ODocument outVertex;
          try {
            outVertex = out.getRecord();
          } catch (ORecordNotFoundException e) {
            outVertex = null;
          }

          if (outVertex == null) outVertexMissing = true;
          else {
            final String outFieldName =
                OrientVertex.getConnectionFieldName(
                    Direction.OUT, edge.getClassName(), useVertexFieldsForEdgeLabels);

            final Object outEdges = outVertex.field(outFieldName);
            if (outEdges == null) outVertexMissing = true;
            else if (outEdges instanceof ORidBag) {
              if (!((ORidBag) outEdges).contains(edgeId)) outVertexMissing = true;
            } else if (outEdges instanceof Collection) {
              if (!((Collection) outEdges).contains(edgeId)) outVertexMissing = true;
            } else if (outEdges instanceof OIdentifiable) {
              if (((OIdentifiable) outEdges).getIdentity().equals(edgeId)) outVertexMissing = true;
            }
          }
        }

        if (outVertexMissing)
          removalReason = "outgoing vertex (" + out + ") does not contain the edge";

        boolean inVertexMissing = false;

        final OIdentifiable in = OrientEdge.getConnection(edge, Direction.IN);
        if (in == null) inVertexMissing = true;
        else {

          ODocument inVertex;
          try {
            inVertex = in.getRecord();
          } catch (ORecordNotFoundException e) {
            inVertex = null;
          }

          if (inVertex == null) inVertexMissing = true;
          else {
            final String inFieldName =
                OrientVertex.getConnectionFieldName(
                    Direction.IN, edge.getClassName(), useVertexFieldsForEdgeLabels);

            final Object inEdges = inVertex.field(inFieldName);
            if (inEdges == null) inVertexMissing = true;
            else if (inEdges instanceof ORidBag) {
              if (!((ORidBag) inEdges).contains(edgeId)) inVertexMissing = true;
            } else if (inEdges instanceof Collection) {
              if (!((Collection) inEdges).contains(edgeId)) inVertexMissing = true;
            } else if (inEdges instanceof OIdentifiable) {
              if (((OIdentifiable) inEdges).getIdentity().equals(edgeId)) inVertexMissing = true;
            }
          }
        }

        if (inVertexMissing) {
          if (!removalReason.isEmpty()) removalReason += ", ";
          removalReason += "incoming vertex (" + in + ") does not contain the edge";
        }

        if (outVertexMissing || inVertexMissing) {
          try {
            if (!checkOnly) {
              message(
                  outputListener,
                  "+ deleting corrupted edge " + edge + " because " + removalReason + "\n");
              edge.delete();
            } else
              message(
                  outputListener,
                  "+ found corrupted edge " + edge + " because " + removalReason + "\n");

            stats.removedEdges++;
            if (eventListener != null) eventListener.onRemovedEdge(edge);

          } catch (Exception e) {
            message(
                outputListener,
                "Error on deleting edge " + edge.getIdentity() + " (" + e.getMessage() + ")");
          }
        }
      }
      message(outputListener, "Scanning edges completed\n");
    }
  }

  protected void repairVertices(
      final OrientBaseGraph graph,
      final ORepairStats stats,
      final OCommandOutputListener outputListener,
      final Map<String, List<String>> options,
      final boolean checkOnly) {
    final ODatabaseDocument db = graph.getRawGraph();
    final OMetadata metadata = db.getMetadata();
    final OSchema schema = metadata.getSchema();

    final OClass vertexClass = schema.getClass(OrientVertexType.CLASS_NAME);
    if (vertexClass != null) {
      final long countVertices = db.countClass(vertexClass.getName());

      long skipVertices = 0l;
      if (options != null && options.get("-skipVertices") != null) {
        skipVertices = Long.parseLong(options.get("-skipVertices").get(0));
      }

      message(outputListener, "Scanning " + countVertices + " vertices...\n");

      long parsedVertices = 0l;
      final long beginTime = System.currentTimeMillis();

      for (ODocument vertex : db.browseClass(vertexClass.getName())) {
        parsedVertices++;
        if (skipVertices > 0 && parsedVertices <= skipVertices) continue;

        stats.scannedVertices++;
        if (eventListener != null) eventListener.onScannedVertex(vertex);

        if (outputListener != null && stats.scannedVertices % 100000 == 0) {
          long speedPerSecond =
              (long) (parsedVertices / ((System.currentTimeMillis() - beginTime) / 1000.0));
          if (speedPerSecond < 1) speedPerSecond = 1;
          final long remaining = (countVertices - parsedVertices) / speedPerSecond;

          message(
              outputListener,
              "+ vertices: scanned "
                  + stats.scannedVertices
                  + ", repaired "
                  + stats.repairedVertices
                  + " (estimated remaining time "
                  + remaining
                  + " secs)\n");
        }

        final OrientVertex v = graph.getVertex(vertex);

        boolean vertexCorrupted = false;
        for (String fieldName : vertex.fieldNames()) {
          final OPair<Direction, String> connection =
              v.getConnection(Direction.BOTH, fieldName, null);
          if (connection == null)
            // SKIP THIS FIELD
            continue;

          final Object fieldValue = vertex.rawField(fieldName);
          if (fieldValue != null) {
            if (fieldValue instanceof OIdentifiable) {

              if (isEdgeBroken(
                  vertex,
                  fieldName,
                  connection.getKey(),
                  (OIdentifiable) fieldValue,
                  stats,
                  graph.settings.isUseVertexFieldsForEdgeLabels())) {
                vertexCorrupted = true;
                if (!checkOnly) {
                  vertex.field(fieldName, (Object) null);
                } else
                  message(
                      outputListener,
                      "+ found corrupted vertex "
                          + vertex
                          + " the property "
                          + fieldName
                          + " could be removed\n");
              }

            } else if (fieldValue instanceof Collection<?>) {

              final Collection<?> coll = ((Collection<?>) fieldValue);
              for (Iterator<?> it = coll.iterator(); it.hasNext(); ) {
                final Object o = it.next();

                if (isEdgeBroken(
                    vertex,
                    fieldName,
                    connection.getKey(),
                    (OIdentifiable) o,
                    stats,
                    graph.settings.isUseVertexFieldsForEdgeLabels())) {
                  vertexCorrupted = true;
                  if (!checkOnly) {
                    it.remove();
                  } else
                    message(
                        outputListener,
                        "+ found corrupted vertex "
                            + vertex
                            + " the edge should be removed from property "
                            + fieldName
                            + " (collection)\n");
                }
              }

            } else if (fieldValue instanceof ORidBag) {
              // In case of ridbags force save for trigger eventual conversions
              final ORidBag ridbag = ((ORidBag) fieldValue);
              if (ridbag.size() == 0) {
                vertex.removeField(fieldName);
              } else if (!ridbag.isEmbedded()
                  && ridbag.size()
                      < OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD
                          .getValueAsInteger()) {
                vertex.setDirty();
              }
              for (Iterator<?> it = ridbag.rawIterator(); it.hasNext(); ) {
                final Object o = it.next();
                if (isEdgeBroken(
                    vertex,
                    fieldName,
                    connection.getKey(),
                    (OIdentifiable) o,
                    stats,
                    graph.settings.isUseVertexFieldsForEdgeLabels())) {
                  vertexCorrupted = true;
                  if (!checkOnly) {
                    it.remove();
                  } else
                    message(
                        outputListener,
                        "+ found corrupted vertex "
                            + vertex
                            + " the edge should be removed from property "
                            + fieldName
                            + " (ridbag)\n");
                }
              }
            }
          }
        }

        if (vertexCorrupted) {
          stats.repairedVertices++;
          if (eventListener != null) eventListener.onRepairedVertex(vertex);

          message(outputListener, "+ repaired corrupted vertex " + vertex + "\n");
          if (!checkOnly) {
            vertex.save();
          }
        } else if (vertex.isDirty() && !checkOnly) {
          message(outputListener, "+ optimized vertex " + vertex + "\n");
          vertex.save();
        }
      }

      message(outputListener, "Scanning vertices completed\n");
    }
  }

  private void onScannedLink(final ORepairStats stats, final OIdentifiable fieldValue) {
    stats.scannedLinks++;
    if (eventListener != null) eventListener.onScannedLink(fieldValue);
  }

  private void onRemovedLink(final ORepairStats stats, final OIdentifiable fieldValue) {
    stats.removedLinks++;
    if (eventListener != null) eventListener.onRemovedLink(fieldValue);
  }

  public OStorageRecoverEventListener getEventListener() {
    return eventListener;
  }

  public OGraphRepair setEventListener(final OStorageRecoverEventListener eventListener) {
    this.eventListener = eventListener;
    return this;
  }

  private void message(final OCommandOutputListener outputListener, final String message) {
    if (outputListener != null) outputListener.onMessage(message);
  }

  private boolean isEdgeBroken(
      final OIdentifiable vertex,
      final String fieldName,
      final Direction direction,
      final OIdentifiable edgeRID,
      final ORepairStats stats,
      final boolean useVertexFieldsForEdgeLabels) {
    onScannedLink(stats, edgeRID);

    boolean broken = false;

    if (edgeRID == null)
      // RID NULL
      broken = true;
    else {
      ODocument record = null;
      try {
        record = edgeRID.getIdentity().getRecord();
      } catch (ORecordNotFoundException e) {
        broken = true;
      }

      if (record == null)
        // RECORD DELETED
        broken = true;
      else {
        final OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(record);
        if (immutableClass == null
            || (!immutableClass.isVertexType() && !immutableClass.isEdgeType()))
          // INVALID RECORD TYPE: NULL OR NOT GRAPH TYPE
          broken = true;
        else {
          if (immutableClass.isVertexType()) {
            // VERTEX -> LIGHTWEIGHT EDGE
            final String inverseFieldName =
                OrientVertex.getInverseConnectionFieldName(fieldName, useVertexFieldsForEdgeLabels);

            // CHECK THE VERTEX IS IN INVERSE EDGE CONTAINS
            final Object inverseEdgeContainer = record.field(inverseFieldName);
            if (inverseEdgeContainer == null)
              // NULL CONTAINER
              broken = true;
            else {

              if (inverseEdgeContainer instanceof OIdentifiable) {
                if (!inverseEdgeContainer.equals(vertex))
                  // NOT THE SAME
                  broken = true;
              } else if (inverseEdgeContainer instanceof Collection<?>) {
                if (!((Collection) inverseEdgeContainer).contains(vertex))
                  // NOT IN COLLECTION
                  broken = true;

              } else if (inverseEdgeContainer instanceof ORidBag) {
                if (!((ORidBag) inverseEdgeContainer).contains(vertex))
                  // NOT IN RIDBAG
                  broken = true;
              }
            }
          } else {
            // EDGE -> REGULAR EDGE, OK
            final OIdentifiable backRID = OrientEdge.getConnection(record, direction);
            if (backRID == null || !backRID.equals(vertex))
              // BACK RID POINTS TO ANOTHER VERTEX
              broken = true;
          }
        }
      }
    }

    if (broken) {
      onRemovedLink(stats, edgeRID);
      return true;
    }

    return false;
  }
}
