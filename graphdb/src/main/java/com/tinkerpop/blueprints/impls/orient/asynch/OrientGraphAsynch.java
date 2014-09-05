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

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
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
  private final Features                              FEATURES           = new Features();
  private final OrientGraphFactory                    factory;
  private ConcurrentLinkedHashMap<ORID, OrientVertex> vertexCache;
  private int                                         maxPoolSize        = 32;
  private int                                         maxRetries         = 16;
  private boolean                                     transactional      = false;
  private AtomicLong                                  operationStarted   = new AtomicLong();
  private AtomicLong                                  operationCompleted = new AtomicLong();

  public OrientGraphAsynch(final String url) {
    factory = new OrientGraphFactory(url).setupPool(1, maxPoolSize).setTransactional(transactional);
  }

  public OrientGraphAsynch(final String url, final String username, final String password) {
    factory = new OrientGraphFactory(url, username, password).setupPool(1, maxPoolSize).setTransactional(transactional);
  }

  public OrientGraphAsynch setCache(final long iElements) {
    vertexCache = new ConcurrentLinkedHashMap.Builder<ORID, OrientVertex>().maximumWeightedCapacity(iElements).build();
    return this;
  }

  public Vertex addVertex(final Object id, final Object... prop) {
    final long operationId = operationStarted.incrementAndGet();

    return new OrientVertexFuture(Orient.instance().getWorkers().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          try {
            final OrientVertex v = g.addVertex(id, prop);

            if (vertexCache != null)
              vertexCache.put(v.getIdentity(), v);

            return v;

          } catch (ORecordDuplicatedException e) {
            // ALREADY EXISTS, TRY TO MERGE IT
            for (int retry = 0;; retry++) {
              if (OLogManager.instance().isDebugEnabled())
                OLogManager.instance().debug(this,
                    "Vertex already created, merge it and retry again (retry=" + retry + "/" + maxRetries + ")");

              boolean modified = false;
              final ODocument existent = e.getRid().getRecord();
              for (int i = 0; i < prop.length; i += 2) {
                final String pName = prop[i].toString();
                final Object pValue = prop[i + 1];

                final Object fieldValue = existent.field(pName);
                if (fieldValue == null || (!fieldValue.equals(pValue))) {
                  // OVERWRITE PROPERTY VALUE
                  existent.field(pName, fieldValue);
                  modified = true;
                }
              }

              if (modified) {
                try {
                  existent.save();

                } catch (ONeedRetryException ex) {
                  if (retry < maxRetries)
                    existent.reload(null, true);
                  else
                    throw e;
                }
              }

              final OrientVertex v = (OrientVertex) getVertex(existent);

              if (vertexCache != null)
                vertexCache.put(v.getIdentity(), v);

              return v;
            }
          }
        } finally {
          g.shutdown();
          asynchOperationCompleted();
        }
      }
    }));
  }

  @Override
  public Vertex addVertex(final Object id) {
    final long operationId = operationStarted.incrementAndGet();

    return new OrientVertexFuture(Orient.instance().getWorkers().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          final OrientVertex v = g.addVertex(id);

          if (vertexCache != null)
            vertexCache.put(v.getIdentity(), v);

          return v;
        } finally {
          g.shutdown();
          asynchOperationCompleted();
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
    if (id instanceof OrientVertex)
      return (Vertex) id;

    final OrientBaseGraph g = acquire();
    try {
      if (id != null)
        return null;

      OrientVertex v;
      if (vertexCache != null) {
        if (id instanceof OIdentifiable) {
          OIdentifiable objId = (OIdentifiable) id;
          v = vertexCache.get(objId.getIdentity());
          if (v != null)
            // FOUND
            return v;
        }
      }

      // LOAD FROM DATABASE AND STORE IN CACHE
      v = g.getVertex(id);
      if (v != null && vertexCache != null)
        vertexCache.put(v.getIdentity(), v);

      return v;

    } finally {
      g.shutdown();
    }
  }

  @Override
  public void removeVertex(final Vertex vertex) {
    final long operationId = operationStarted.incrementAndGet();

    Orient.instance().getWorkers().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          if (vertexCache != null)
            vertexCache.remove(vertex.getId());

          g.removeVertex(vertex);
        } finally {
          g.shutdown();
          asynchOperationCompleted();
        }
        return null;
      }
    });
  }

  @Override
  public Iterable<Vertex> getVertices() {

    final OrientBaseGraph g = acquire();
    try {
      return g.getVertices();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public Iterable<Vertex> getVertices(final String key, final Object value) {

    final OrientBaseGraph g = acquire();
    try {
      return g.getVertices(key, value);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
    final long operationId = operationStarted.incrementAndGet();

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
            } catch (Exception e) {
              if (retry < maxRetries) {
                if (OLogManager.instance().isDebugEnabled())
                  OLogManager.instance().debug(
                      this,
                      "Conflict on addEdge(" + id + "," + outVertex + "," + inVertex + "," + label + "), retrying (retry=" + retry
                          + "/" + maxRetries + ")");
                vOut.getRecord().reload(null, true);
                vIn.getRecord().reload(null, true);
              } else
                throw e;
            }
          }
        } finally {
          g.shutdown();
          asynchOperationCompleted();
        }
      }
    }));
  }

  @Override
  public Edge getEdge(final Object id) {

    final OrientBaseGraph g = acquire();
    try {
      return g.getEdge(id);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public void removeEdge(final Edge edge) {
    final long operationId = operationStarted.incrementAndGet();

    Orient.instance().getWorkers().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          g.removeEdge(edge);
        } finally {
          g.shutdown();
          asynchOperationCompleted();
        }
        return null;
      }
    });
  }

  @Override
  public Iterable<Edge> getEdges() {

    final OrientBaseGraph g = acquire();
    try {
      return g.getEdges();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public Iterable<Edge> getEdges(final String key, final Object value) {

    final OrientBaseGraph g = acquire();
    try {
      return g.getEdges(key, value);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public void drop() {
    final OrientBaseGraph g = acquire();
    try {
      g.drop();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertex addTemporaryVertex(final String iClassName, final Object... prop) {
    final OrientBaseGraph g = acquire();
    try {
      return g.addTemporaryVertex(iClassName, prop);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertexType getVertexBaseType() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertexBaseType();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertexType getVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getVertexType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName, final String iSuperClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName, iSuperClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientVertexType createVertexType(final String iClassName, final OClass iSuperClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createVertexType(iClassName, iSuperClass);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public void dropVertexType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropVertexType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientEdgeType getEdgeBaseType() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdgeBaseType();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientEdgeType getEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getEdgeType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName, final String iSuperClassName) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName, iSuperClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientEdgeType createEdgeType(final String iClassName, final OClass iSuperClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.createEdgeType(iClassName, iSuperClass);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public void dropEdgeType(final String iClassName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropEdgeType(iClassName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientElement detach(final OrientElement iElement) {
    final OrientBaseGraph g = acquire();
    try {
      return g.detach(iElement);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public OrientElement attach(final OrientElement iElement) {
    final OrientBaseGraph g = acquire();
    try {
      return g.attach(iElement);
    } finally {
      g.shutdown();
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
    synchronized (operationCompleted) {
      while (operationStarted.get() > operationCompleted.get()) {
        try {
          operationCompleted.wait();
        } catch (InterruptedException e) {
          Thread.interrupted();
          break;
        }
      }
    }

    if (vertexCache != null)
      vertexCache.clear();

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
      g.shutdown();
    }
  }

  @Override
  public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndex(indexName, indexClass);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public Iterable<Index<? extends Element>> getIndices() {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndices();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public void dropIndex(final String indexName) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropIndex(indexName);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass) {
    final OrientBaseGraph g = acquire();
    try {
      g.dropKeyIndex(key, elementClass);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, final Parameter... indexParameters) {
    final OrientBaseGraph g = acquire();
    try {
      g.createKeyIndex(key, elementClass, indexParameters);
    } finally {
      g.shutdown();
    }
  }

  @Override
  public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
    final OrientBaseGraph g = acquire();
    try {
      return g.getIndexedKeys(elementClass);
    } finally {
      g.shutdown();
    }
  }

  public OrientBaseGraph acquire() {
    return factory.get();
  }

  public OCommandRequest command(final OCommandSQL iCommand) {

    final OrientBaseGraph g = acquire();
    try {
      return g.command(iCommand);
    } finally {

      g.shutdown();
    }
  }

  public long countVertices() {
    final OrientBaseGraph g = acquire();
    try {
      return g.countVertices();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public long countVertices(String iClassName) {

    final OrientBaseGraph g = acquire();
    try {
      return g.countVertices(iClassName);
    } finally {
      g.shutdown();
    }
  }

  public long countEdges() {

    final OrientBaseGraph g = acquire();
    try {
      return g.countEdges();
    } finally {
      g.shutdown();
    }
  }

  @Override
  public long countEdges(String iClassName) {

    final OrientBaseGraph g = acquire();
    try {
      return g.countEdges(iClassName);
    } finally {
      g.shutdown();
    }
  }

  public <V> V execute(final OCallable<V, OrientBaseGraph> iCallable) {
    final OrientBaseGraph graph = acquire();
    try {
      return iCallable.call(graph);
    } finally {
      graph.shutdown();
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

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(final int maxRetries) {
    this.maxRetries = maxRetries;
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

  private void asynchOperationCompleted() {
    operationCompleted.incrementAndGet();
    synchronized (operationCompleted) {
      operationCompleted.notifyAll();
    }
  }
}
