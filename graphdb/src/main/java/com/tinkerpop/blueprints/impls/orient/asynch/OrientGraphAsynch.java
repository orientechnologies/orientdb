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

package com.tinkerpop.blueprints.impls.orient.asynch;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientExtendedGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com) that uses multi-threading to work
 * against graph.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraphAsynch implements OrientExtendedGraph {
  private final Features           FEATURES         = new Features();
  private final OrientGraphFactory factory;
  private final AtomicLong         updateOperations = new AtomicLong();
  private int                      maxPoolSize      = 100;
  private boolean                  transactional    = false;

  public OrientGraphAsynch(final String url) {
    factory = new OrientGraphFactory(url).setupPool(1, maxPoolSize).setTransactional(transactional);
  }

  public OrientGraphAsynch(final String url, final String username, final String password) {
    factory = new OrientGraphFactory(url, username, password).setupPool(1, maxPoolSize).setTransactional(transactional);
  }

  public Vertex addVertex(final Object id, final Object... prop) {
    final long operationId = beginGraphUpdate();
    return new OrientVertexFuture(Orient.instance().getWorkers().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          return g.addVertex(id, prop);
        } finally {
          endGraphUpdate(operationId);
          release(g);
        }
      }
    }));
  }

  @Override
  public Vertex addVertex(final Object id) {
    final long operationId = beginGraphUpdate();
    return new OrientVertexFuture(Orient.instance().getWorkers().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          return g.addVertex(id);
        } finally {
          endGraphUpdate(operationId);
          release(g);
        }
      }
    }));
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    factory.declareIntent(iIntent);
  }

  @Override
  public Vertex getVertex(final Object id) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertex(id);
    } finally {
      release(g);
    }
  }

  @Override
  public void removeVertex(final Vertex vertex) {
    final long operationId = beginGraphUpdate();
    Orient.instance().getWorkers().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          g.removeVertex(vertex);
        } finally {
          endGraphUpdate(operationId);
          release(g);
        }
        return null;
      }
    });
  }

  @Override
  public Iterable<Vertex> getVertices() {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertices();
    } finally {
      release(g);
    }
  }

  @Override
  public Iterable<Vertex> getVertices(final String key, final Object value) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertices(key, value);
    } finally {
      release(g);
    }
  }

  @Override
  public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
    final long operationId = beginGraphUpdate();
    return new OrientEdgeFuture(Orient.instance().getWorkers().submit(new Callable<OrientEdge>() {
      @Override
      public OrientEdge call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          OrientVertex vOut = outVertex instanceof OrientVertexFuture ? ((OrientVertexFuture) outVertex).get()
              : (OrientVertex) outVertex;
          OrientVertex vIn = inVertex instanceof OrientVertexFuture ? ((OrientVertexFuture) inVertex).get()
              : (OrientVertex) inVertex;

          vOut.attach(g);
          vIn.attach(g);

          for (int retry = 0;; retry++) {
            try {
              return g.addEdge(id, vOut, vIn, label);
            } catch (ONeedRetryException e) {
              if (retry < 20) {
                OLogManager.instance().debug(this,
                    "Conflict on addEdge(" + id + "," + outVertex + "," + inVertex + "," + label + "), retrying " + retry);
                vOut.getRecord().reload();
                vIn.getRecord().reload();
              } else
                throw e;
            }
          }
        } finally {
          endGraphUpdate(operationId);
          release(g);
        }
      }
    }));
  }

  @Override
  public Edge getEdge(final Object id) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdge(id);
    } finally {
      release(g);
    }
  }

  @Override
  public void removeEdge(final Edge edge) {
    final long operationId = beginGraphUpdate();
    Orient.instance().getWorkers().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          g.removeEdge(edge);
        } finally {
          endGraphUpdate(operationId);
          release(g);
        }
        return null;
      }
    });
  }

  @Override
  public Iterable<Edge> getEdges() {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdges();
    } finally {
      release(g);
    }
  }

  @Override
  public Iterable<Edge> getEdges(final String key, final Object value) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdges(key, value);
    } finally {
      release(g);
    }
  }

  @Override
  public void drop() {
    final OrientBaseGraph g = acquire();
    try {
      g.drop();
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertex addTemporaryVertex(final String iClassName, final Object... prop) {
    final OrientBaseGraph g = acquire();
    try {
      return g.addTemporaryVertex(iClassName, prop);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertexType getVertexBaseType() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertexBaseType();
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertexType getVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertexType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName, final String iSuperClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName, iSuperClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName, final OClass iSuperClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName, iSuperClass);
    } finally {
      release(g);
    }
  }

  @Override
  public void dropVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropVertexType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientEdgeType getEdgeBaseType() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdgeBaseType();
    } finally {
      release(g);
    }
  }

  @Override
  public OrientEdgeType getEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdgeType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName, final String iSuperClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName, iSuperClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName, final OClass iSuperClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName, iSuperClass);
    } finally {
      release(g);
    }
  }

  @Override
  public void dropEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropEdgeType(iClassName);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientElement detach(final OrientElement iElement) {
    final OrientBaseGraph g = acquire();
    try {
      return g.detach(iElement);
    } finally {
      release(g);
    }
  }

  @Override
  public OrientElement attach(final OrientElement iElement) {
    final OrientBaseGraph g = acquire();
    try {
      return g.attach(iElement);
    } finally {
      release(g);
    }
  }

  @Override
  public GraphQuery query() {
    throw new UnsupportedOperationException("query");
  }

  @Override
  public OTraverse traverse() {
    throw new UnsupportedOperationException("traverse");
  }

  @Override
  public OCommandRequest command(final OCommandRequest iCommand) {
    throw new UnsupportedOperationException("command");
  }

  @Override
  public void shutdown() {
    factory.close();
  }

  public Features getFeatures() {
    return FEATURES;
  }

  @Override
  public <T extends Element> Index<T> createIndex(final String indexName, final Class<T> indexClass,
      final Parameter... indexParameters) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createIndex(indexName, indexClass, indexParameters);
    } finally {
      release(g);
    }
  }

  @Override
  public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndex(indexName, indexClass);
    } finally {
      release(g);
    }
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndices();
    } finally {
      release(g);
    }
  }

  @Override
  public void dropIndex(final String indexName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropIndex(indexName);
    } finally {
      release(g);
    }
  }

  @Override
  public <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropKeyIndex(key, elementClass);
    } finally {
      release(g);
    }
  }

  @Override
  public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, final Parameter... indexParameters) {
    final OrientBaseGraph g = acquire();
    try {
      g.createKeyIndex(key, elementClass, indexParameters);
    } finally {
      release(g);
    }
  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndexedKeys(elementClass);
    } finally {
      release(g);
    }
  }

  public OrientBaseGraph acquire() {
    return factory.get();
  }

  public void release(final OrientBaseGraph iGraph) {
    factory.release(iGraph);
  }

  public OCommandRequest command(final OCommandSQL iCommand) {
    final long operationId = beginGraphUpdate();
    final OrientBaseGraph g = acquire();
    try {
      return g.command(iCommand);
    } finally {
      endGraphUpdate(operationId);
      release(g);
    }
  }

  public long countVertices() {
    final OrientBaseGraph g = acquire();
    try {
      return g.countVertices();
    } finally {
      release(g);
    }
  }

  @Override
  public long countVertices(String iClassName) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.countVertices(iClassName);
    } finally {
      release(g);
    }
  }

  public long countEdges() {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.countEdges();
    } finally {
      release(g);
    }
  }

  @Override
  public long countEdges(String iClassName) {
    waitUntilCompletition(updateOperations.get());
    final OrientBaseGraph g = acquire();
    try {
      return g.countEdges(iClassName);
    } finally {
      release(g);
    }
  }

  public <V> V execute(final OCallable<V, OrientBaseGraph> iCallable) {
    final OrientBaseGraph graph = factory.get();
    try {
      return iCallable.call(graph);
    } finally {
      factory.release(graph);
    }
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(final int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public boolean isTransactional() {
    return transactional;
  }

  public void setTransactional(final boolean transactional) {
    this.transactional = transactional;
  }

  @Override
  public ODatabaseDocumentTx getRawGraph() {
    throw new UnsupportedOperationException("getRawGraph");
  }

  protected void config() {
    FEATURES.supportsDuplicateEdges = true;
    FEATURES.supportsSelfLoops = true;
    FEATURES.isPersistent = true;
    FEATURES.supportsVertexIteration = true;
    FEATURES.supportsVertexIndex = true;
    FEATURES.ignoresSuppliedIds = true;
    FEATURES.supportsTransactions = false;
    FEATURES.supportsVertexKeyIndex = true;
    FEATURES.supportsKeyIndices = true;
    FEATURES.isWrapper = false;
    FEATURES.supportsIndices = true;
    FEATURES.supportsVertexProperties = true;
    FEATURES.supportsEdgeProperties = true;

    // For more information on supported types, please see:
    // http://code.google.com/p/orient/wiki/Types
    FEATURES.supportsSerializableObjectProperty = true;
    FEATURES.supportsBooleanProperty = true;
    FEATURES.supportsDoubleProperty = true;
    FEATURES.supportsFloatProperty = true;
    FEATURES.supportsIntegerProperty = true;
    FEATURES.supportsPrimitiveArrayProperty = true;
    FEATURES.supportsUniformListProperty = true;
    FEATURES.supportsMixedListProperty = true;
    FEATURES.supportsLongProperty = true;
    FEATURES.supportsMapProperty = true;
    FEATURES.supportsStringProperty = true;
    FEATURES.supportsThreadedTransactions = false;
  }

  private long beginGraphUpdate() {
    return updateOperations.incrementAndGet();
  }

  private void endGraphUpdate(final long operationId) {
  }

  private void waitUntilCompletition(final long iWaitForOperationId) {
  }
}
