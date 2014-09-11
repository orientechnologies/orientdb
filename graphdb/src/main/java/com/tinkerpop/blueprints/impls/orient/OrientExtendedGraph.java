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
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Vertex;

/**
 * OrientDB extension to Blueprints standard.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public interface OrientExtendedGraph extends IndexableGraph, MetaGraph<ODatabaseDocumentTx>, KeyIndexableGraph {

  /**
   * (Blueprints Extension) Drops the database
   */
  public void drop();

  /**
   * (Blueprints Extension) Creates a temporary vertex setting the initial field values. The vertex is not saved and the transaction
   * is not started.
   * 
   * @param iClassName
   *          Vertex's class name
   * @param prop
   *          Fields must be a odd pairs of key/value or a single object as Map containing entries as key/value pairs
   * @return added vertex
   */
  public OrientVertex addTemporaryVertex(String iClassName, Object... prop);

  /**
   * (Blueprints Extension) Creates a new unconnected vertex in the Graph setting the initial field values.
   *
   * @param id
   *          Optional, can contains the Edge's class name by prefixing with "class:"
   * @param prop
   *          Fields must be a odd pairs of key/value or a single object as Map containing entries as key/value pairs
   * @return The new OrientVertex created
   */
  public Vertex addVertex(Object id, Object... prop);

  /**
   * Returns the V persistent class as OrientVertexType instance.
   */
  public OrientVertexType getVertexBaseType();

  /**
   * Returns the persistent class for type iTypeName as OrientVertexType instance.
   * 
   * @param iTypeName
   *          Vertex class name
   */
  public OrientVertexType getVertexType(String iTypeName);

  /**
   * Creates a new Vertex persistent class.
   * 
   * @param iClassName
   *          Vertex class name
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(String iClassName);

  /**
   * Creates a new Vertex persistent class specifying the super class.
   * 
   * @param iClassName
   *          Vertex class name
   * @param iSuperClassName
   *          Vertex class name to extend
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(String iClassName, String iSuperClassName);

  /**
   * Creates a new Vertex persistent class specifying the super class.
   * 
   * @param iClassName
   *          Vertex class name
   * @param iSuperClass
   *          OClass Vertex to extend
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(String iClassName, OClass iSuperClass);

  /**
   * Drop a vertex class.
   * 
   * @param iTypeName
   *          Vertex class name
   */
  public void dropVertexType(String iTypeName);

  /**
   * Returns the E persistent class as OrientEdgeType instance.
   */
  public OrientEdgeType getEdgeBaseType();

  /**
   * Returns the persistent class for type iTypeName as OrientEdgeType instance.
   * 
   * @param iTypeName
   *          Edge class name
   */
  public OrientEdgeType getEdgeType(String iTypeName);

  /**
   * Creates a new Edge persistent class.
   * 
   * @param iClassName
   *          Edge class name
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(String iClassName);

  /**
   * Creates a new Edge persistent class specifying the super class.
   * 
   * @param iClassName
   *          Edge class name
   * @param iSuperClassName
   *          Edge class name to extend
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(String iClassName, String iSuperClassName);

  /**
   * Creates a new Edge persistent class specifying the super class.
   * 
   * @param iClassName
   *          Edge class name
   * @param iSuperClass
   *          OClass Edge to extend
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(String iClassName, OClass iSuperClass);

  /**
   * Drops an edge class.
   * 
   * @param iTypeName
   *          Edge class name
   */
  public void dropEdgeType(String iTypeName);

  /**
   * Detaches a Graph Element to be used offline. All the changes will be committed on further @attach call.
   * 
   * @param iElement
   *          Graph element to detach
   * @return The detached element
   * @see #attach(OrientElement)
   */
  public OrientElement detach(OrientElement iElement);

  /**
   * Attaches a previously detached Graph Element to the current Graph. All the pending changes will be committed.
   * 
   * @param iElement
   *          Graph element to attach
   * @return The attached element
   * @see #detach(OrientElement)
   */
  public OrientElement attach(OrientElement iElement);

  /**
   * Returns a GraphQuery object to execute queries against the Graph.
   * 
   * @return new GraphQuery instance
   */
  @Override
  public GraphQuery query();

  /**
   * Returns a OTraverse object to start traversing the graph.
   */
  public OTraverse traverse();

  /**
   * Executes commands against the graph. Commands are executed outside transaction.
   * 
   * @param iCommand
   *          Command request between SQL, GREMLIN and SCRIPT commands
   */
  public OCommandRequest command(OCommandRequest iCommand);

  /**
   * Counts the vertices in graph.
   * 
   * @return Long as number of total vertices
   */
  public long countVertices();

  /**
   * Counts the vertices in graph of a particular class.
   * 
   * @return Long as number of total vertices
   */
  public long countVertices(String iClassName);

  /**
   * Counts the edges in graph. Edge counting works only if useLightweightEdges is false.
   * 
   * @return Long as number of total edges
   */
  public long countEdges();

  /**
   * Counts the edges in graph of a particular class. Edge counting works only if useLightweightEdges is false.
   * 
   * @return Long as number of total edges
   */
  public long countEdges(String iClassName);

  /**
   * Declare an intent.
   */
  public void declareIntent(OIntent iIntent);

  public ORecordConflictStrategy getConflictStrategy();

  public OrientExtendedGraph setConflictStrategy(ORecordConflictStrategy iResolver);

  public OrientExtendedGraph setConflictStrategy(String iStrategyName);
}
