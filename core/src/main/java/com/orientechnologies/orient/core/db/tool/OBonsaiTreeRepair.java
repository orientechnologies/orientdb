package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.OVertexDocument;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Find and repair broken bonsai tree removing the double linked buckets and regenerating the whole
 * tree with data from referring records.
 *
 * @author tglman
 */
public class OBonsaiTreeRepair {

  public void repairDatabaseRidbags(ODatabaseDocument db, OCommandOutputListener outputListener) {
    message(outputListener, "Repair of ridbags is started ...\n");

    final OMetadata metadata = db.getMetadata();
    final OSchema schema = metadata.getSchema();
    final OClass edgeClass = schema.getClass("E");
    if (edgeClass != null) {
      final HashMap<String, Set<ORID>> processedVertexes = new HashMap<String, Set<ORID>>();
      final long countEdges = db.countClass(edgeClass.getName());

      message(outputListener, countEdges + " will be processed.");
      long counter = 0;

      for (ODocument edge : db.browseClass(edgeClass.getName())) {
        try {
          final String label;
          if (edge.field("label") != null) {
            label = edge.field("label");
          } else if (!edge.getClassName().equals(edgeClass.getName())) {
            label = edge.getClassName();
          } else {
            counter++;
            continue;
          }

          OIdentifiable inId = edge.<OIdentifiable>field("in");
          OIdentifiable outId = edge.<OIdentifiable>field("out");
          if (inId == null || outId == null) {
            db.delete(edge);
            continue;
          }
          final ODocument inVertex = inId.getRecord();
          final ODocument outVertex = outId.getRecord();

          final String inVertexName =
              OVertexDocument.getConnectionFieldName(ODirection.IN, label, true);
          final String outVertexName =
              OVertexDocument.getConnectionFieldName(ODirection.OUT, label, true);

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
            message(
                outputListener, counter + " edges were processed out of " + countEdges + " \n.");

        } catch (Exception e) {
          final StringWriter sw = new StringWriter();

          sw.append("Error during processing of edge with id ")
              .append(edge.getIdentity().toString())
              .append("\n");
          e.printStackTrace(new PrintWriter(sw));

          message(outputListener, sw.toString());
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
