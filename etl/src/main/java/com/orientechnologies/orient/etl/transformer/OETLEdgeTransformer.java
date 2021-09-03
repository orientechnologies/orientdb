/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class OETLEdgeTransformer extends OETLAbstractLookupTransformer {
  private String edgeClass = "E";
  private boolean directionOut = true;
  private ODocument targetVertexFields;
  private ODocument edgeFields;
  private boolean skipDuplicates = false;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:["
                + getCommonConfigurationParameters()
                + ","
                + "{joinValue:{optional:true,description:'value to use for join'}},"
                + "{joinFieldName:{optional:true,description:'field name containing the value to join'}},"
                + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
                + "{direction:{optional:true,description:'Direction between \'in\' and \'out\'. Default is \'out\''}},"
                + "{class:{optional:true,description:'Edge class name. Default is \'E\''}},"
                + "{targetVertexFields:{optional:true,description:'Map of fields to set in target vertex. Use ${$input.<field>} to get input field values'}},"
                + "{edgeFields:{optional:true,description:'Map of fields to set in edge. Use ${$input.<field>} to get input field values'}},"
                + "{skipDuplicates:{optional:true,description:'Duplicated edges (with a composite index built on both out and in properties) are skipped', default:false}},"
                + "{unresolvedLinkAction:{optional:true,description:'action when the target vertex is not found',values:"
                + stringArray2Json(ACTION.values())
                + "}}],"
                + "input:['ODocument','OVertex'],output:'OVertex'}");
  }

  @Override
  public void configure(final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iConfiguration, iContext);
    edgeClass = iConfiguration.field("class");
    if (iConfiguration.containsField("direction")) {
      final String direction = iConfiguration.field("direction");
      if ("out".equalsIgnoreCase(direction)) directionOut = true;
      else if ("in".equalsIgnoreCase(direction)) directionOut = false;
      else
        throw new OConfigurationException(
            "Direction can be 'in' or 'out', but found: " + direction);
    }

    if (iConfiguration.containsField("targetVertexFields"))
      targetVertexFields = (ODocument) iConfiguration.field("targetVertexFields");
    if (iConfiguration.containsField("edgeFields"))
      edgeFields = (ODocument) iConfiguration.field("edgeFields");
    if (iConfiguration.containsField("skipDuplicates"))
      skipDuplicates = (Boolean) resolve(iConfiguration.field("skipDuplicates"));
  }

  @Override
  public String getName() {
    return "edge";
  }

  @Override
  public void begin(ODatabaseDocument db) {
    super.begin(db);
    final OClass cls = db.getClass(edgeClass);
    if (cls == null) db.createEdgeClass(edgeClass);
    super.begin(db);
  }

  @Override
  public Object executeTransform(ODatabaseDocument db, final Object input) {
    for (Object o : OMultiValue.getMultiValueIterable(input)) {
      // GET JOIN VALUE
      final OVertex vertex;
      if (o instanceof OVertex) vertex = (OVertex) o;
      else if (o instanceof OIdentifiable)
        vertex = ((OElement) db.getRecord((OIdentifiable) o)).asVertex().get();
      else
        throw new OETLTransformException(getName() + ": input type '" + o + "' is not supported");

      Object joinCurrentValue = joinValue;
      if (joinCurrentValue == null) {
        if (joinFieldName.startsWith("$")) joinCurrentValue = resolve(joinFieldName);
        else joinCurrentValue = vertex.getProperty(joinFieldName);
      }

      if (OMultiValue.isMultiValue(joinCurrentValue)) {
        // RESOLVE SINGLE JOINS
        for (Object ob : OMultiValue.getMultiValueIterable(joinCurrentValue)) {
          final Object r = lookup((ODatabaseDocumentInternal) db, ob, true);
          if (createEdge(db, vertex, ob, r) == null) {
            if (unresolvedLinkAction == ACTION.SKIP)
              // RETURN NULL ONLY IN CASE SKIP ACTION IS REQUESTED
              return null;
          }
        }
      } else {
        final Object result = lookup((ODatabaseDocumentInternal) db, joinCurrentValue, true);
        if (createEdge(db, vertex, joinCurrentValue, result) == null) {
          if (unresolvedLinkAction == ACTION.SKIP)
            // RETURN NULL ONLY IN CASE SKIP ACTION IS REQUESTED
            return null;
        }
      }
    }

    return input;
  }

  private List<OEdge> createEdge(
      ODatabaseDocument db, final OVertex vertex, final Object joinCurrentValue, Object result) {
    log(Level.FINE, "joinCurrentValue=%s, lookupResult=%s", joinCurrentValue, result);

    if (result == null) {
      // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
      switch (unresolvedLinkAction) {
        case CREATE:
          // Don't try to create a Vertex with a null value
          if (joinCurrentValue != null) {
            if (lookup != null) {
              final String[] lookupParts = lookup.split("\\.");
              final OVertex linkedV = db.newVertex(lookupParts[0]);
              linkedV.setProperty(lookupParts[1], joinCurrentValue);

              if (targetVertexFields != null) {
                for (String f : targetVertexFields.fieldNames())
                  linkedV.setProperty(f, resolve(targetVertexFields.field(f)));
              }

              linkedV.save();

              log(Level.FINE, "created new vertex=" + linkedV.getRecord());

              result = linkedV.getIdentity();
            } else {
              throw new OConfigurationException(
                  "Cannot create linked document because target class is unknown. Use 'lookup' field");
            }
          }
          break;
        case ERROR:
          processor.getStats().incrementErrors();
          log(
              Level.SEVERE,
              "%s: ERROR Cannot resolve join for value '%s'",
              getName(),
              joinCurrentValue);
          break;
        case WARNING:
          processor.getStats().incrementWarnings();
          log(
              Level.INFO,
              "%s: WARN Cannot resolve join for value '%s'",
              getName(),
              joinCurrentValue);
          break;
        case SKIP:
          return null;
        case HALT:
          throw new OETLProcessHaltedException(
              "Cannot resolve join for value '" + joinCurrentValue + "'");
        case NOTHING:
        default:
          return null;
      }
    }

    if (result != null) {
      final List<OEdge> edges;
      if (OMultiValue.isMultiValue(result)) {
        final int size = OMultiValue.getSize(result);
        if (size == 0)
          // NO EDGES
          return null;

        edges = new ArrayList<OEdge>(size);
      } else edges = new ArrayList<OEdge>(1);

      for (Object o : OMultiValue.getMultiValueIterable(result)) {
        OIdentifiable oid = (OIdentifiable) o;
        final OVertex targetVertex = ((OElement) db.getRecord(oid)).asVertex().get();

        try {
          // CREATE THE EDGE
          final OEdge edge;
          if (directionOut) edge = (OEdge) vertex.addEdge(targetVertex, edgeClass);
          else edge = (OEdge) targetVertex.addEdge(vertex, edgeClass);

          if (edgeFields != null) {
            for (String f : edgeFields.fieldNames())
              edge.setProperty(f, resolve(edgeFields.field(f)));
          }

          edges.add(edge);
          log(Level.FINE, "created new edge=%s", edge);
        } catch (ORecordDuplicatedException e) {
          if (skipDuplicates) {
            log(Level.FINE, "skipped creation of new edge because already exists");
            continue;
          } else {
            log(
                Level.SEVERE,
                "error on creation of new edge because it already exists (skipDuplicates=false)");
            throw e;
          }
        }
      }

      edges.stream().forEach(e -> db.save(e));

      return edges;
    }

    // NO EDGES
    return null;
  }
}
