/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OEdgeTransformer extends OAbstractLookupTransformer {
  private String    edgeClass    = "E";
  private boolean   directionOut = true;
  private ODocument targetVertexFields;
  private ODocument edgeFields;

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON("{parameters:["
            + getCommonConfigurationParameters()
            + ","
            + "{joinValue:{optional:true,description:'value to use for join'}},"
            + "{joinFieldName:{optional:true,description:'field name containing the value to join'}},"
            + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
            + "{direction:{optional:true,description:'Direction between \'in\' and \'out\'. Default is \'out\''}},"
            + "{class:{optional:true,description:'Edge class name. Default is \'E\''}},"
            + "{targetVertexFields:{optional:true,description:'Map of fields to set in target vertex. Use ${$input.<field>} to get input field values'}},"
            + "{edgeFields:{optional:true,description:'Map of fields to set in edge. Use ${input.<field>} to get input field values'}},"
            + "{unresolvedVertexAction:{optional:true,description:'action when a unresolved vertices is found',values:"
            + stringArray2Json(ACTION.values()) + "}}]," + "input:['ODocument','OrientVertex'],output:'OrientVertex'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    edgeClass = iConfiguration.field("class");
    if (iConfiguration.containsField("direction")) {
      final String direction = iConfiguration.field("direction");
      if ("out".equalsIgnoreCase(direction))
        directionOut = true;
      else if ("in".equalsIgnoreCase(direction))
        directionOut = false;
      else
        throw new OConfigurationException("Direction can be 'in' or 'out', but found: " + direction);
    }

    if (iConfiguration.containsField("targetVertexFields"))
      targetVertexFields = (ODocument) iConfiguration.field("targetVertexFields");
    if (iConfiguration.containsField("edgeFields"))
      edgeFields = (ODocument) iConfiguration.field("edgeFields");
  }

  @Override
  public String getName() {
    return "edge";
  }

  @Override
  public void begin() {
    final OClass cls = pipeline.getGraphDatabase().getEdgeType(edgeClass);
    if (cls == null)
      pipeline.getGraphDatabase().createEdgeType(edgeClass);
    super.begin();
  }

  @Override
  public Object executeTransform(final Object input) {
    // GET JOIN VALUE
    final OrientVertex vertex;
    if (input instanceof OrientVertex)
      vertex = (OrientVertex) input;
    else if (input instanceof OIdentifiable)
      vertex = pipeline.getGraphDatabase().getVertex(input);
    else
      throw new OTransformException(getName() + ": input type '" + input + "' is not supported");

    final Object joinCurrentValue = joinValue != null ? joinValue : vertex.getProperty(joinFieldName);

    if (OMultiValue.isMultiValue(joinCurrentValue)) {
      // RESOLVE SINGLE JOINS
      for (Object o : OMultiValue.getMultiValueIterable(joinCurrentValue)) {
        final Object r = lookup(o, false);
        if (createEdge(vertex, o, r) == null) {
          if (unresolvedLinkAction == ACTION.SKIP)
            // RETURN NULL ONLY IN CASE SKIP ACTION IS REQUESTED
            return null;
        }
      }
    } else {
      final Object result = lookup(joinCurrentValue, false);
      if (createEdge(vertex, joinCurrentValue, result) == null) {
        if (unresolvedLinkAction == ACTION.SKIP)
          // RETURN NULL ONLY IN CASE SKIP ACTION IS REQUESTED
          return null;
      }
    }
    return input;
  }

  private OrientEdge createEdge(final OrientVertex vertex, final Object joinCurrentValue, Object result) {
    log(OETLProcessor.LOG_LEVELS.DEBUG, "joinCurrentValue=%s, lookupResult=%s", joinCurrentValue, result);

    if (result == null) {
      // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
      switch (unresolvedLinkAction) {
      case CREATE:
        //Don't try to create a Vertex with a null value
        if (joinCurrentValue != null) {
          if (lookup != null) {
            final String[] lookupParts = lookup.split("\\.");
            final OrientVertex linkedV = pipeline.getGraphDatabase().addTemporaryVertex(lookupParts[0]);
            linkedV.setProperty(lookupParts[1], joinCurrentValue);

            if (targetVertexFields != null) {
              for (String f : targetVertexFields.fieldNames())
                linkedV.setProperty(f, resolve(targetVertexFields.field(f)));
            }

            linkedV.save();

            log(OETLProcessor.LOG_LEVELS.DEBUG, "created new vertex=%s", linkedV.getRecord());

            result = linkedV;
          } else {
            throw new OConfigurationException("Cannot create linked document because target class is unknown. Use 'lookup' field");
          }
        }
        break;
      case ERROR:
        processor.getStats().incrementErrors();
        log(OETLProcessor.LOG_LEVELS.ERROR, "%s: ERROR Cannot resolve join for value '%s'", getName(), joinCurrentValue);
        break;
      case WARNING:
        processor.getStats().incrementWarnings();
        log(OETLProcessor.LOG_LEVELS.INFO, "%s: WARN Cannot resolve join for value '%s'", getName(), joinCurrentValue);
        break;
      case SKIP:
        return null;
      case HALT:
        throw new OETLProcessHaltedException("Cannot resolve join for value '" + joinCurrentValue + "'");
      }
    }

    if (result != null) {
      final OrientVertex targetVertex = pipeline.getGraphDatabase().getVertex(result);

      // CREATE THE EDGE
      final OrientEdge edge;
      if (directionOut)
        edge = (OrientEdge) vertex.addEdge(edgeClass, targetVertex);
      else
        edge = (OrientEdge) targetVertex.addEdge(edgeClass, vertex);

      if (edgeFields != null) {
        for (String f : edgeFields.fieldNames())
          edge.setProperty(f, resolve(edgeFields.field(f)));
      }

      log(OETLProcessor.LOG_LEVELS.DEBUG, "created new edge=%s", edge);
      return edge;
    }

    return null;
  }
}
