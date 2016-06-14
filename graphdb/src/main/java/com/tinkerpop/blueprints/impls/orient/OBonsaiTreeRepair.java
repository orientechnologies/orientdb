package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Find and repair broken bonsai tree removing the double linked buckets and regenerating the whole tree with data from referring
 * records.
 * 
 * 
 * @author tglman
 *
 */
public class OBonsaiTreeRepair {

  public void repairDatabaseRidbags(ODatabaseDocument db, OCommandOutputListener outputListener) {
    message(outputListener, "Repair of ridbags is started ...\n");

    final OMetadata metadata = db.getMetadata();
    final OSchema schema = metadata.getSchema();
    final OClass edgeClass = schema.getClass(OrientEdgeType.CLASS_NAME);
    if (edgeClass != null) {
      final HashMap<String, Set<ORID>> processedVertexes = new HashMap<String, Set<ORID>>();
      final long countEdges = db.countClass(edgeClass.getName());

      message(outputListener, countEdges + " will be processed.");
      long counter = 0;

      for (ODocument edge : db.browseClass(edgeClass.getName())) {
        try {
          final String label;
          if (edge.field(OrientElement.LABEL_FIELD_NAME) != null) {
            label = edge.field(OrientElement.LABEL_FIELD_NAME);
          } else if (!edge.getClassName().equals(edgeClass.getName())) {
            label = edge.getClassName();
          } else {
            counter++;
            continue;
          }

          final ODocument inVertex = edge.<OIdentifiable> field(OrientBaseGraph.CONNECTION_IN).getRecord();
          final ODocument outVertex = edge.<OIdentifiable> field(OrientBaseGraph.CONNECTION_OUT).getRecord();

          final String inVertexName = OrientVertex.getConnectionFieldName(Direction.IN, label, true);
          final String outVertexName = OrientVertex.getConnectionFieldName(Direction.OUT, label, true);

          Set<ORID> inVertexes = processedVertexes.get(inVertexName);
          if (inVertexes == null) {
            inVertexes = new HashSet<ORID>();
            processedVertexes.put(inVertexName, inVertexes);
          }

          Set<ORID> outVertexes = processedVertexes.get(outVertexName);
          if (outVertexes == null) {
            outVertexes = new HashSet<ORID>();
            processedVertexes.put(outVertexName, outVertexes);
          }

          if (inVertex.field(inVertexName) instanceof ORidBag) {
            if (inVertexes.add(inVertex.getIdentity())) {
              inVertex.field(inVertexName, new ORidBag());
            }

            final ORidBag inRidBag = inVertex.field(inVertexName);
            inRidBag.add(edge.getIdentity());

            inVertex.save();
          }

          if (outVertex.field(outVertexName) instanceof ORidBag) {
            if (outVertexes.add(outVertex.getIdentity())) {
              outVertex.field(outVertexName, new ORidBag());
            }

            final ORidBag outRidBag = outVertex.field(outVertexName);
            outRidBag.add(edge.getIdentity());

            outVertex.save();
          }

          counter++;

          if (counter > 0 && counter % 1000 == 0)
            message(outputListener, counter + " edges were processed out of " + countEdges + " \n.");

        } catch (Exception e) {
          e.printStackTrace();
          message(outputListener, "Error during processing of edge with id " + edge.getIdentity() + "\n");
        }
      }

      message(outputListener, "Processed " + counter + " from " + countEdges + ".");
    }

    message(outputListener, "repair of ridbags is completed\n");
  }

  private void message(OCommandOutputListener outputListener, String message) {
    if (outputListener != null) {
      outputListener.onMessage(message);
    }
  }
}
