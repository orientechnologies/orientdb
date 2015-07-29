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
package com.orientechnologies.orient.graph.script;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientConfigurableGraph.THREAD_MODE;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Blueprints Graph wrapper class to use from scripts.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OScriptGraphWrapper implements TransactionalGraph {
  protected OrientBaseGraph graph;

  public OScriptGraphWrapper(final OrientBaseGraph iWrapped) {
    graph = iWrapped;
  }

  public Object command(final String language, final String iText) {
    return command(language, iText, null);
  }

  @SuppressWarnings("unchecked")
  public Object command(final String language, final String iText, final Object[] iArgs) {
    Object result;
    if (language.equalsIgnoreCase("sql"))
      result = graph.command(new OCommandSQL(iText)).execute(iArgs);
    else if (language.equalsIgnoreCase("gremlin"))
      result = graph.command(new OCommandGremlin(iText)).execute(iArgs);
    else
      result = graph.command(new OCommandScript(language, iText)).execute(iArgs);

    if (result instanceof Iterable<?>) {
      // FOR SAKE OF SIMPLICITY TRANSFORM ANY ITERABLE IN ARRAY
      final List<Object> list = new ArrayList<Object>();
      for (Object o : (Iterable<Object>) result) {
        list.add(o);
      }
      result = list.toArray();
    }

    return result;
  }

  @Override
  public Features getFeatures() {
    return graph.getFeatures();
  }

  public int hashCode() {
    return graph.hashCode();
  }

  public void commit() {
    graph.commit();
  }

  public void rollback() {
    graph.rollback();
  }

  public boolean isAutoStartTx() {
    if (graph instanceof OrientGraph)
      return graph.isAutoStartTx();

    return false;
  }

  public void setAutoStartTx(boolean autoStartTx) {
    if (graph instanceof OrientGraph)
      graph.setAutoStartTx(autoStartTx);
  }

  public void stopTransaction(Conclusion conclusion) {
    if (graph instanceof OrientGraph)
      ((OrientGraph) graph).stopTransaction(conclusion);
  }

  public boolean equals(final Object obj) {
    return graph.equals(obj);
  }

  public void drop() {
    graph.drop();
  }

  public <T extends Element> Index<T> createIndex(final String indexName, Class<T> indexClass, Parameter... indexParameters) {
    return graph.createIndex(indexName, indexClass, indexParameters);
  }

  public <T extends Element> Index<T> getIndex(final String indexName, Class<T> indexClass) {
    return graph.getIndex(indexName, indexClass);
  }

  public Iterable<Index<? extends Element>> getIndices() {
    return graph.getIndices();
  }

  public void dropIndex(final String indexName) {
    graph.dropIndex(indexName);
  }

  public OrientVertex addVertex(Object id) {
    return graph.addVertex(id);
  }

  public OrientVertex addVertex(Object id, Object[] prop) {
    return graph.addVertex(id, prop);
  }

  public OrientVertex addTemporaryVertex(final String iClassName, Object[] prop) {
    return graph.addTemporaryVertex(iClassName, prop);
  }

  public OrientEdge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
    return graph.addEdge(id, outVertex, inVertex, label);
  }

  public OrientVertex getVertex(Object id) {
    return graph.getVertex(id);
  }

  public void removeVertex(Vertex vertex) {
    graph.removeVertex(vertex);
  }

  public Iterable<Vertex> getVertices() {
    return graph.getVertices();
  }

  public Iterable<Vertex> getVertices(boolean iPolymorphic) {
    return graph.getVertices(iPolymorphic);
  }

  public Iterable<Vertex> getVerticesOfClass(final String iClassName) {
    return graph.getVerticesOfClass(iClassName);
  }

  public Iterable<Vertex> getVerticesOfClass(final String iClassName, boolean iPolymorphic) {
    return graph.getVerticesOfClass(iClassName, iPolymorphic);
  }

  public Iterable<Vertex> getVertices(final String iKey, Object iValue) {
    return graph.getVertices(iKey, iValue);
  }

  public Iterable<Edge> getEdges() {
    return graph.getEdges();
  }

  public Iterable<Edge> getEdges(boolean iPolymorphic) {
    return graph.getEdges(iPolymorphic);
  }

  public Iterable<Edge> getEdgesOfClass(final String iClassName) {
    return graph.getEdgesOfClass(iClassName);
  }

  public Iterable<Edge> getEdgesOfClass(final String iClassName, boolean iPolymorphic) {
    return graph.getEdgesOfClass(iClassName, iPolymorphic);
  }

  public Iterable<Edge> getEdges(final String iKey, final Object iValue) {
    return graph.getEdges(iKey, iValue);
  }

  public OrientEdge getEdge(final Object id) {
    return graph.getEdge(id);
  }

  public void removeEdge(final Edge edge) {
    graph.removeEdge(edge);
  }

  public OrientBaseGraph reuse(final ODatabaseDocumentTx iDatabase) {
    return graph.reuse(iDatabase);
  }

  public boolean isClosed() {
    return graph.isClosed();
  }

  public void shutdown() {
    graph.shutdown();
  }

  public String toString() {
    return graph.toString();
  }

  public ODatabaseDocumentTx getRawGraph() {
    return graph.getRawGraph();
  }

  public OClass getVertexBaseType() {
    return graph.getVertexBaseType();
  }

  public final OClass getVertexType(final String iTypeName) {
    return graph.getVertexType(iTypeName);
  }

  public OClass createVertexType(final String iClassName) {
    return graph.createVertexType(iClassName);
  }

  public OClass createVertexType(final String iClassName, String iSuperClassName) {
    return graph.createVertexType(iClassName, iSuperClassName);
  }

  public OClass createVertexType(final String iClassName, OClass iSuperClass) {
    return graph.createVertexType(iClassName, iSuperClass);
  }

  public final void dropVertexType(final String iTypeName) {
    graph.dropVertexType(iTypeName);
  }

  public OClass getEdgeBaseType() {
    return graph.getEdgeBaseType();
  }

  public final OClass getEdgeType(final String iTypeName) {
    return graph.getEdgeType(iTypeName);
  }

  public OClass createEdgeType(final String iClassName) {
    return graph.createEdgeType(iClassName);
  }

  public OClass createEdgeType(final String iClassName, String iSuperClassName) {
    return graph.createEdgeType(iClassName, iSuperClassName);
  }

  public OClass createEdgeType(final String iClassName, OClass iSuperClass) {
    return graph.createEdgeType(iClassName, iSuperClass);
  }

  public final void dropEdgeType(final String iTypeName) {
    graph.dropEdgeType(iTypeName);
  }

  public OrientElement getElement(Object id) {
    return graph.getElement(id);
  }

  public <T extends Element> void dropKeyIndex(final String key, Class<T> elementClass) {
    graph.dropKeyIndex(key, elementClass);
  }

  public <T extends Element> void createKeyIndex(final String key, Class<T> elementClass, Parameter... indexParameters) {
    graph.createKeyIndex(key, elementClass, indexParameters);
  }

  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    return graph.getIndexedKeys(elementClass);
  }

  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass, boolean includeClassNames) {
    return graph.getIndexedKeys(elementClass, includeClassNames);
  }

  public GraphQuery query() {
    return graph.query();
  }

  public OTraverse traverse() {
    return graph.traverse();
  }

  public OCommandRequest command(OCommandRequest iCommand) {
    return graph.command(iCommand);
  }

  public boolean isUseLightweightEdges() {
    return graph.isUseLightweightEdges();
  }

  public void setUseLightweightEdges(boolean useDynamicEdges) {
    graph.setUseLightweightEdges(useDynamicEdges);
  }

  public boolean isSaveOriginalIds() {
    return graph.isSaveOriginalIds();
  }

  public void setSaveOriginalIds(boolean saveIds) {
    graph.setSaveOriginalIds(saveIds);
  }

  public long countVertices() {
    return graph.countVertices();
  }

  public long countVertices(final String iClassName) {
    return graph.countVertices(iClassName);
  }

  public long countEdges() {
    return graph.countEdges();
  }

  public long countEdges(final String iClassName) {
    return graph.countEdges(iClassName);
  }

  public boolean isKeepInMemoryReferences() {
    return graph.isKeepInMemoryReferences();
  }

  public void setKeepInMemoryReferences(boolean useReferences) {
    graph.setKeepInMemoryReferences(useReferences);
  }

  public boolean isUseClassForEdgeLabel() {
    return graph.isUseClassForEdgeLabel();
  }

  public void setUseClassForEdgeLabel(boolean useCustomClassesForEdges) {
    graph.setUseClassForEdgeLabel(useCustomClassesForEdges);
  }

  public boolean isUseClassForVertexLabel() {
    return graph.isUseClassForVertexLabel();
  }

  public void setUseClassForVertexLabel(boolean useCustomClassesForVertex) {
    graph.setUseClassForVertexLabel(useCustomClassesForVertex);
  }

  public boolean isUseVertexFieldsForEdgeLabels() {
    return graph.isUseVertexFieldsForEdgeLabels();
  }

  public void setUseVertexFieldsForEdgeLabels(boolean useVertexFieldsForEdgeLabels) {
    graph.setUseVertexFieldsForEdgeLabels(useVertexFieldsForEdgeLabels);
  }

  public boolean isStandardElementConstraints() {
    return graph.isStandardElementConstraints();
  }

  public void setStandardElementConstraints(boolean allowsPropertyValueNull) {
    graph.setStandardElementConstraints(allowsPropertyValueNull);
  }

  public THREAD_MODE getThreadMode() {
    return graph.getThreadMode();
  }

  public OrientBaseGraph setThreadMode(THREAD_MODE iControl) {
    return (OrientBaseGraph) graph.setThreadMode(iControl);
  }

}
