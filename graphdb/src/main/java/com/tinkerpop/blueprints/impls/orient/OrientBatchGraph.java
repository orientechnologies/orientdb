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

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

import java.util.Set;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com) super fast on massive insertion.
 * Extends Blueprints BatchGraph class and behavior.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientBatchGraph extends BatchGraph<OrientGraph> implements OrientExtendedGraph {

  public OrientBatchGraph(final OrientGraph graph, final VertexIDType type, final long bufferSize) {
    super(graph, type, bufferSize);
    init();
  }

  public OrientBatchGraph(final OrientGraph graph, final long bufferSize) {
    super(graph, bufferSize);
    init();
  }

  public OrientBatchGraph(final OrientGraph graph) {
    super(graph);
    init();
  }

  @Override
  public void drop() {
    getBaseGraph().drop();
  }

  @Override
  public OrientVertex addTemporaryVertex(String iClassName, Object... prop) {
    return getBaseGraph().addTemporaryVertex(iClassName, prop);
  }

  @Override
  public OrientVertexType getVertexBaseType() {
    return getBaseGraph().getVertexBaseType();
  }

  @Override
  public OrientVertexType getVertexType(String iTypeName) {
    return getBaseGraph().getVertexType(iTypeName);
  }

  @Override
  public OrientVertexType createVertexType(String iClassName) {
    return getBaseGraph().createVertexType(iClassName);
  }

  @Override
  public OrientVertexType createVertexType(String iClassName, String iSuperClassName) {
    return getBaseGraph().createVertexType(iClassName, iSuperClassName);
  }

  @Override
  public OrientVertexType createVertexType(String iClassName, OClass iSuperClass) {
    return getBaseGraph().createVertexType(iClassName, iSuperClass);
  }

  @Override
  public void dropVertexType(String iTypeName) {
    getBaseGraph().dropVertexType(iTypeName);
  }

  @Override
  public OrientEdgeType getEdgeBaseType() {
    return getBaseGraph().getEdgeBaseType();
  }

  @Override
  public OrientEdgeType getEdgeType(String iTypeName) {
    return getBaseGraph().getEdgeType(iTypeName);
  }

  @Override
  public OrientEdgeType createEdgeType(String iClassName) {
    return getBaseGraph().createEdgeType(iClassName);
  }

  @Override
  public OrientEdgeType createEdgeType(String iClassName, String iSuperClassName) {
    return getBaseGraph().createEdgeType(iClassName, iSuperClassName);
  }

  @Override
  public OrientEdgeType createEdgeType(String iClassName, OClass iSuperClass) {
    return getBaseGraph().createEdgeType(iClassName, iSuperClass);
  }

  @Override
  public void dropEdgeType(String iTypeName) {
    getBaseGraph().dropEdgeType(iTypeName);
  }

  @Override
  public OrientElement detach(OrientElement iElement) {
    return getBaseGraph().detach(iElement);
  }

  @Override
  public OrientElement attach(OrientElement iElement) {
    return getBaseGraph().attach(iElement);
  }

  @Override
  public OTraverse traverse() {
    return getBaseGraph().traverse();
  }

  @Override
  public OCommandRequest command(OCommandRequest iCommand) {
    return getBaseGraph().command(iCommand);
  }

  @Override
  public long countVertices() {
    return getBaseGraph().countVertices();
  }

  @Override
  public long countVertices(String iClassName) {
    return getBaseGraph().countVertices(iClassName);
  }

  @Override
  public long countEdges() {
    return getBaseGraph().countEdges();
  }

  @Override
  public long countEdges(String iClassName) {
    return getBaseGraph().countEdges(iClassName);
  }

  @Override
  public void declareIntent(OIntent iIntent) {
    getBaseGraph().declareIntent(iIntent);
  }

  @Override
  public <T extends Element> Index<T> createIndex(String indexName, Class<T> indexClass, Parameter... indexParameters) {
    return getBaseGraph().createIndex(indexName, indexClass, indexParameters);
  }

  @Override
  public <T extends Element> Index<T> getIndex(String indexName, Class<T> indexClass) {
    return getBaseGraph().getIndex(indexName, indexClass);
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    return getBaseGraph().getIndices();
  }

  @Override
  public void dropIndex(String indexName) {
    getBaseGraph().dropIndex(indexName);
  }

  @Override
  public <T extends Element> void dropKeyIndex(String key, Class<T> elementClass) {
    getBaseGraph().dropKeyIndex(key, elementClass);

  }

  @Override
  public <T extends Element> void createKeyIndex(String key, Class<T> elementClass, Parameter... indexParameters) {
    getBaseGraph().createKeyIndex(key, elementClass, indexParameters);
  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
    return getBaseGraph().getIndexedKeys(elementClass);
  }

  @Override
  public ODatabaseDocumentTx getRawGraph() {
    return getBaseGraph().getRawGraph();
  }

  private void init() {
    getBaseGraph().setUseLog(false);
  }
}
