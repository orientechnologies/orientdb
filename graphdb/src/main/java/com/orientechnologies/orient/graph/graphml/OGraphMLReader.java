/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *  
 */

package com.orientechnologies.orient.graph.graphml;

import com.orientechnologies.orient.core.db.tool.ODatabaseImportException;
import com.orientechnologies.orient.core.id.ORID;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLTokens;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * GraphMLReader writes the data from a GraphML stream to a graph. Derived from Blueprints GraphMLReader. Supports also vertex
 * labels.
 *
 * @author Luca Garulli (l.garulli(at)orientechnologies.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Alex Averbuch (alex.averbuch-at-gmail.com)
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class OGraphMLReader {
  private static final String                 LABELS              = "labels";
  private final OrientBaseGraph               graph;
  private int                                 vertexLabelIndex    = 0;
  private String                              vertexIdKey         = "id";
  private String                              edgeIdKey           = null;
  private String                              edgeLabelKey        = GraphMLTokens.LABEL;
  private boolean                             storeVertexIds      = false;
  private int                                 batchSize           = 1000;
  private Map<String, OGraphMLImportStrategy> vertexPropsStrategy = new HashMap<String, OGraphMLImportStrategy>();
  private Map<String, OGraphMLImportStrategy> edgePropsStrategy   = new HashMap<String, OGraphMLImportStrategy>();

  /**
   * @param graph
   *          the graph to populate with the GraphML data
   */
  public OGraphMLReader(OrientBaseGraph graph) {
    this.graph = graph;
  }

  /**
   * Define custom strategy to use for vertex attribute.
   * 
   * @param iAttributeName
   *          attribute name
   * @param iStrategy
   *          strategy implementation
   */
  public OGraphMLReader defineVertexAttributeStrategy(final String iAttributeName, final OGraphMLImportStrategy iStrategy) {
    vertexPropsStrategy.put(iAttributeName, iStrategy);
    return this;
  }

  /**
   * Define custom strategy to use for edge attribute.
   *
   * @param iAttributeName
   *          attribute name
   * @param iStrategy
   *          strategy implementation
   */
  public OGraphMLReader defineEdgeAttributeStrategy(final String iAttributeName, final OGraphMLImportStrategy iStrategy) {
    edgePropsStrategy.put(iAttributeName, iStrategy);
    return this;
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param inputGraph
   *          the graph to populate with the GraphML data
   * @param graphMLInputStream
   *          an InputStream of GraphML data
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public void inputGraph(final Graph inputGraph, final InputStream graphMLInputStream) throws IOException {
    inputGraph(inputGraph, graphMLInputStream, batchSize, vertexIdKey, edgeIdKey, edgeLabelKey);
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param inputGraph
   *          the graph to populate with the GraphML data
   * @param filename
   *          name of a file containing GraphML data
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public void inputGraph(final Graph inputGraph, final String filename) throws IOException {
    inputGraph(inputGraph, filename, batchSize, vertexIdKey, edgeIdKey, edgeLabelKey);
  }

  /**
   * Input the GraphML stream data into the graph. More control over how data is streamed is provided by this method.
   *
   * @param inputGraph
   *          the graph to populate with the GraphML data
   * @param filename
   *          name of a file containing GraphML data
   * @param bufferSize
   *          the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
   * @param vertexIdKey
   *          if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
   * @param edgeIdKey
   *          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
   * @param edgeLabelKey
   *          if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final Graph inputGraph, final String filename, int bufferSize, String vertexIdKey,
      String edgeIdKey, String edgeLabelKey) throws IOException {
    FileInputStream fis = new FileInputStream(filename);
    try {
      return inputGraph(inputGraph, fis, bufferSize, vertexIdKey, edgeIdKey, edgeLabelKey);
    } finally {
      fis.close();
    }
  }

  /**
   * Input the GraphML stream data into the graph. More control over how data is streamed is provided by this method.
   *
   * @param inputGraph
   *          the graph to populate with the GraphML data
   * @param graphMLInputStream
   *          an InputStream of GraphML data
   * @param bufferSize
   *          the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
   * @param vertexIdKey
   *          if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
   * @param edgeIdKey
   *          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
   * @param edgeLabelKey
   *          if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final Graph inputGraph, final InputStream graphMLInputStream, int bufferSize,
      String vertexIdKey, String edgeIdKey, String edgeLabelKey) throws IOException {

    XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    try {
      XMLStreamReader reader = inputFactory.createXMLStreamReader(graphMLInputStream);

      final OrientBaseGraph graph = (OrientBaseGraph) inputGraph;

      if (storeVertexIds)
        graph.setSaveOriginalIds(storeVertexIds);

      Map<String, String> keyIdMap = new HashMap<String, String>();
      Map<String, String> keyTypesMaps = new HashMap<String, String>();
      // <Mapped ID String, ID Object>

      // <Default ID String, Mapped ID String>
      Map<String, ORID> vertexMappedIdMap = new HashMap<String, ORID>();

      // Buffered Vertex Data
      String vertexId = null;
      Map<String, Object> vertexProps = null;
      boolean inVertex = false;

      // Buffered Edge Data
      String edgeId = null;
      String edgeLabel = null;
      String vertexLabel = null;
      Vertex[] edgeEndVertices = null; // [0] = outVertex , [1] = inVertex
      Map<String, Object> edgeProps = null;
      boolean inEdge = false;

      int bufferCounter = 0;

      while (reader.hasNext()) {

        Integer eventType = reader.next();
        if (eventType.equals(XMLEvent.START_ELEMENT)) {
          String elementName = reader.getName().getLocalPart();

          if (elementName.equals(GraphMLTokens.KEY)) {
            String id = reader.getAttributeValue(null, GraphMLTokens.ID);
            String attributeName = reader.getAttributeValue(null, GraphMLTokens.ATTR_NAME);
            String attributeType = reader.getAttributeValue(null, GraphMLTokens.ATTR_TYPE);
            keyIdMap.put(id, attributeName);
            keyTypesMaps.put(id, attributeType);

          } else if (elementName.equals(GraphMLTokens.NODE)) {
            vertexId = reader.getAttributeValue(null, GraphMLTokens.ID);
            vertexLabel = reader.getAttributeValue(null, LABELS);
            if (vertexLabel != null) {
              if (vertexLabel.startsWith(":"))
                // REMOVE : AS PREFIX
                vertexLabel = vertexLabel.substring(1);

              final String[] vertexLabels = vertexLabel.split(":");

              // GET ONLY FIRST LABEL AS CLASS
              vertexLabel = vertexId + ",class:" + vertexLabels[vertexLabelIndex];
            } else
              vertexLabel = vertexId;

            inVertex = true;
            vertexProps = new HashMap<String, Object>();

          } else if (elementName.equals(GraphMLTokens.EDGE)) {
            edgeId = reader.getAttributeValue(null, GraphMLTokens.ID);
            edgeLabel = reader.getAttributeValue(null, GraphMLTokens.LABEL);
            edgeLabel = edgeLabel == null ? GraphMLTokens._DEFAULT : edgeLabel;

            String[] vertexIds = new String[2];
            vertexIds[0] = reader.getAttributeValue(null, GraphMLTokens.SOURCE);
            vertexIds[1] = reader.getAttributeValue(null, GraphMLTokens.TARGET);
            edgeEndVertices = new Vertex[2];

            for (int i = 0; i < 2; i++) { // i=0 => outVertex, i=1 => inVertex
              if (vertexIdKey == null) {
                edgeEndVertices[i] = null;
              } else {
                final Object vId = vertexMappedIdMap.get(vertexIds[i]);
                edgeEndVertices[i] = vId != null ? graph.getVertex(vId) : null;
              }

              if (null == edgeEndVertices[i]) {
                edgeEndVertices[i] = graph.addVertex(vertexLabel);
                if (vertexIdKey != null) {
                  mapId(vertexMappedIdMap, vertexIds[i], (ORID) edgeEndVertices[i].getId());
                }
                bufferCounter++;
              }
            }

            inEdge = true;
            vertexLabel = null;
            edgeProps = new HashMap<String, Object>();

          } else if (elementName.equals(GraphMLTokens.DATA)) {
            String key = reader.getAttributeValue(null, GraphMLTokens.KEY);
            String attributeName = keyIdMap.get(key);

            if (attributeName == null)
              attributeName = key;

            String value = reader.getElementText();

            if (inVertex) {
              if ((vertexIdKey != null) && (key.equals(vertexIdKey))) {
                // Should occur at most once per Vertex
                vertexId = value;
              } else if (attributeName.equalsIgnoreCase(LABELS)) {
                // IGNORE LABELS
              } else {
                final Object attrValue = typeCastValue(key, value, keyTypesMaps);

                final OGraphMLImportStrategy strategy = vertexPropsStrategy.get(attributeName);
                if (strategy != null) {
                  attributeName = strategy.transformAttribute(attributeName, attrValue);
                }

                if (attributeName != null)
                  vertexProps.put(attributeName, attrValue);
              }
            } else if (inEdge) {
              if ((edgeLabelKey != null) && (key.equals(edgeLabelKey)))
                edgeLabel = value;
              else if ((edgeIdKey != null) && (key.equals(edgeIdKey)))
                edgeId = value;
              else {
                final Object attrValue = typeCastValue(key, value, keyTypesMaps);

                final OGraphMLImportStrategy strategy = edgePropsStrategy.get(attributeName);
                if (strategy != null) {
                  attributeName = strategy.transformAttribute(attributeName, attrValue);
                }

                if (attributeName != null)
                  edgeProps.put(attributeName, attrValue);
              }
            }

          }
        } else if (eventType.equals(XMLEvent.END_ELEMENT)) {
          String elementName = reader.getName().getLocalPart();

          if (elementName.equals(GraphMLTokens.NODE)) {
            ORID currentVertex = null;

            if (vertexIdKey != null)
              currentVertex = vertexMappedIdMap.get(vertexId);

            if (currentVertex == null) {
              final OrientVertex v = graph.addVertex(vertexLabel, vertexProps);
              if (vertexIdKey != null)
                mapId(vertexMappedIdMap, vertexId, v.getIdentity());
              bufferCounter++;
            } else {
              // UPDATE IT
              final OrientVertex v = graph.getVertex(currentVertex);
              v.setProperties(vertexProps);
            }

            vertexId = null;
            vertexLabel = null;
            vertexProps = null;
            inVertex = false;
          } else if (elementName.equals(GraphMLTokens.EDGE)) {
            Edge currentEdge = ((OrientVertex) edgeEndVertices[0]).addEdge(null, (OrientVertex) edgeEndVertices[1], edgeLabel,
                null, edgeProps);
            bufferCounter++;

            edgeId = null;
            edgeLabel = null;
            edgeEndVertices = null;
            edgeProps = null;
            inEdge = false;
          }

        }

        if (bufferCounter > bufferSize) {
          graph.commit();
          bufferCounter = 0;
        }
      }

      reader.close();

      graph.commit();

    } catch (Exception xse) {
      throw new ODatabaseImportException(xse);
    }

    return this;
  }

  public int getVertexLabelIndex() {
    return vertexLabelIndex;
  }

  public void setVertexLabelIndex(int vertexLabelIndex) {
    this.vertexLabelIndex = vertexLabelIndex;
  }

  /**
   * @param vertexIdKey
   *          if the id of a vertex is a &lt;data/&gt; property, fetch it from the data property.
   */
  public void setVertexIdKey(String vertexIdKey) {
    this.vertexIdKey = vertexIdKey;
  }

  /**
   * @param edgeIdKey
   *          if the id of an edge is a &lt;data/&gt; property, fetch it from the data property.
   */
  public void setEdgeIdKey(String edgeIdKey) {
    this.edgeIdKey = edgeIdKey;
  }

  /**
   * @param edgeLabelKey
   *          if the label of an edge is a &lt;data/&gt; property, fetch it from the data property.
   */
  public void setEdgeLabelKey(String edgeLabelKey) {
    this.edgeLabelKey = edgeLabelKey;
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param graphMLInputStream
   *          an InputStream of GraphML data
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final InputStream graphMLInputStream) throws IOException {
    return inputGraph(this.graph, graphMLInputStream, batchSize, this.vertexIdKey, this.edgeIdKey, this.edgeLabelKey);
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param filename
   *          name of a file containing GraphML data
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final String filename) throws IOException {
    return inputGraph(this.graph, filename, batchSize, this.vertexIdKey, this.edgeIdKey, this.edgeLabelKey);
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param graphMLInputStream
   *          an InputStream of GraphML data
   * @param bufferSize
   *          the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final InputStream graphMLInputStream, int bufferSize) throws IOException {
    return inputGraph(this.graph, graphMLInputStream, bufferSize, this.vertexIdKey, this.edgeIdKey, this.edgeLabelKey);
  }

  /**
   * Input the GraphML stream data into the graph. In practice, usually the provided graph is empty.
   *
   * @param filename
   *          name of a file containing GraphML data
   * @param bufferSize
   *          the amount of elements to hold in memory before committing a transactions (only valid for TransactionalGraphs)
   * @throws IOException
   *           thrown when the GraphML data is not correctly formatted
   */
  public OGraphMLReader inputGraph(final String filename, final int bufferSize) throws IOException {
    return inputGraph(this.graph, filename, bufferSize, this.vertexIdKey, this.edgeIdKey, this.edgeLabelKey);
  }

  public boolean isStoreVertexIds() {
    return storeVertexIds;
  }

  public void setStoreVertexIds(final boolean storeVertexIds) {
    this.storeVertexIds = storeVertexIds;
  }

  public OGraphMLReader setOptions(final Map<String, List<String>> opts) {
    for (Map.Entry<String, List<String>> opt : opts.entrySet()) {
      if (opt.getKey().equalsIgnoreCase("storeVertexIds")) {
        storeVertexIds = Boolean.parseBoolean(opt.getValue().get(0));
      } else if (opt.getKey().equalsIgnoreCase("batchSize")) {
        batchSize = Integer.parseInt(opt.getValue().get(0));
      }
    }
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(final int batchSize) {
    this.batchSize = batchSize;
  }

  protected void mapId(final Map<String, ORID> vertexMappedIdMap, final String vertexId, final ORID rid) {
    if (vertexMappedIdMap.containsKey(vertexId))
      throw new IllegalArgumentException("Vertex with id '" + vertexId + "' has been already loaded");
    vertexMappedIdMap.put(vertexId, rid);
  }

  private Object typeCastValue(final String key, final String value, final Map<String, String> keyTypes) {
    String type = keyTypes.get(key);
    if (null == type || type.equals(GraphMLTokens.STRING))
      return value;
    else if (type.equals(GraphMLTokens.FLOAT))
      return Float.valueOf(value);
    else if (type.equals(GraphMLTokens.INT))
      return Integer.valueOf(value);
    else if (type.equals(GraphMLTokens.DOUBLE))
      return Double.valueOf(value);
    else if (type.equals(GraphMLTokens.BOOLEAN))
      return Boolean.valueOf(value);
    else if (type.equals(GraphMLTokens.LONG))
      return Long.valueOf(value);
    else
      return value;
  }
}
