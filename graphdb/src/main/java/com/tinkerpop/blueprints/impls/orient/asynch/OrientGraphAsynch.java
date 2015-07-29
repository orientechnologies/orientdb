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
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
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
import com.tinkerpop.blueprints.impls.orient.OrientExtendedVertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

import java.io.PrintStream;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com) that uses multi-threading to work
 * against graph. This API is experimental, subject to removal or change.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
@OApi(enduser = true, maturity = OApi.MATURITY.EXPERIMENTAL)
public class OrientGraphAsynch implements OrientExtendedGraph {
  private final Features                                FEATURES             = new Features();

  private final String                                  url;
  private final String                                  userName;
  private final String                                  userPassword;

  private OrientGraphFactory                            factory;
  private ConcurrentLinkedHashMap<Object, OrientVertex> vertexCache;
  private int                                           maxPoolSize          = 32;
  private int                                           maxRetries           = 16;
  private boolean                                       transactional        = false;
  private AtomicLong                                    operationStarted     = new AtomicLong();
  private final AtomicLong                              operationCompleted   = new AtomicLong();
  private String                                        keyFieldName;
  // STATISTICS
  private AtomicLong                                    reusedCachedVertex   = new AtomicLong();
  private AtomicLong                                    indexUniqueException = new AtomicLong();
  private AtomicLong                                    concurrentException  = new AtomicLong();
  private AtomicLong                                    unknownException     = new AtomicLong();
  private AtomicLong                                    verticesCreated      = new AtomicLong();
  private AtomicLong                                    edgesCreated         = new AtomicLong();
  private AtomicLong                                    verticesLoaded       = new AtomicLong();
  private AtomicLong                                    verticesRemoved      = new AtomicLong();
  private AtomicLong                                    verticesReloaded     = new AtomicLong();
  private PrintStream                                   outStats             = null;
  private ORecordConflictStrategy                       conflictStrategy     = null;

  protected enum MERGE_RESULT {
    MERGED, RETRY, ERROR
  }

  public OrientGraphAsynch(final String url) {
    this.url = url;
    this.userName = OrientBaseGraph.ADMIN;
    this.userPassword = OrientBaseGraph.ADMIN;
  }

  public OrientGraphAsynch(final String url, final String userName, final String userPassword) {
    this.url = url;
    this.userName = userName;
    this.userPassword = userPassword;
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  public void setKeyFieldName(String keyFieldName) {
    this.keyFieldName = keyFieldName;
  }

  public PrintStream getOutStats() {
    return outStats;
  }

  public void setOutStats(PrintStream outStats) {
    this.outStats = outStats;
  }

  public OrientGraphAsynch setCache(final long iElements) {
    init();
    vertexCache = new ConcurrentLinkedHashMap.Builder<Object, OrientVertex>().maximumWeightedCapacity(iElements).build();
    factory.setKeepInMemoryReferences(true);
    return this;
  }

  public Vertex addOrUpdateVertex(final Object id, final Object... prop) {
    beginAsynchOperation();

    return new OrientVertexFuture(Orient.instance().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          OrientVertex v = (OrientVertex) getVertex(id);
          if (v != null) {
            // System.out.printf("\nVertex loaded key=%d, v=%s", id, v);
            for (int retry = 0;; retry++) {
              MERGE_RESULT result = mergeAndSaveRecord(retry, v.getRecord(), prop);
              switch (result) {
              case MERGED:
                return v;
              case ERROR:
                throw new ORecordDuplicatedException("Cannot create a new vertices", v.getIdentity());
              case RETRY:
                if (retry > maxRetries)
                  break;
              }
            }
          }

          v = g.addVertex(id, prop);

          // System.out.printf("\nCreated vertex key=%d, v=%s", id, v);

          verticesCreated.incrementAndGet();

          return v;

        } catch (ORecordDuplicatedException e) {
          // ALREADY EXISTS, TRY TO MERGE IT
          for (int retry = 0;; retry++) {
            indexUniqueException.incrementAndGet();

            if (OLogManager.instance().isDebugEnabled())
              OLogManager.instance().debug(this, "Vertex %s already created, merge it and retry again (retry=%d/%d)", id, retry,
                  maxRetries);

            final ODocument existent = e.getRid().getRecord();

            final MERGE_RESULT result = mergeAndSaveRecord(retry, existent, prop);

            switch (result) {
            case MERGED:
              return (OrientVertex) getVertex(existent);
            case RETRY:
              break;
            case ERROR:
              throw e;
            }
          }
        } finally {
          g.shutdown();
          endAsynchOperation();
        }
      }
    }));
  }

  public Vertex addVertex(final Object id, final Object... prop) {
    beginAsynchOperation();

    return new OrientVertexFuture(Orient.instance().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          OrientVertex v = g.addVertex(id, prop);

          verticesCreated.incrementAndGet();

          return v;

        } finally {
          g.shutdown();
          endAsynchOperation();
        }
      }
    }));
  }

  public OrientVertex getOrAddVertex(final Object id) {
    final OrientBaseGraph g = acquire();
    try {
      OrientVertex v = getFromCache(id);
      if (v != null)
        return v;

      v = g.addVertex(id);

      verticesCreated.incrementAndGet();

      return v;

    } finally {
      g.shutdown();
    }
  }

  @Override
  public Vertex addVertex(final Object id) {
    beginAsynchOperation();

    return new OrientVertexFuture(Orient.instance().submit(new Callable<OrientVertex>() {
      @Override
      public OrientVertex call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          final OrientVertex v = g.addVertex(id);

          verticesCreated.incrementAndGet();

          return v;
        } finally {
          g.shutdown();
          endAsynchOperation();
        }
      }
    }));
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    init();
    factory.declareIntent(iIntent);
  }

  @Override
  public Vertex getVertex(final Object id) {
    if (id == null)
      return null;

    if (id instanceof OrientVertex)
      return (Vertex) id;

    init();

    if (id instanceof OIdentifiable)
      return new OrientVertex((OrientBaseGraph) null, (OIdentifiable) id);

    OrientVertex v = getFromCache(id);
    if (v != null) {
      return v;
    }

    final OrientBaseGraph g = acquire();
    try {
      // LOAD FROM DATABASE AND STORE IN CACHE
      v = (OrientVertex) g.getVertexByKey(keyFieldName, id);
      if (v != null) {
        verticesLoaded.incrementAndGet();
      }

      return v;

    } finally {
      g.shutdown();
    }
  }

  @Override
  public void removeVertex(final Vertex vertex) {
    beginAsynchOperation();

    Orient.instance().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          if (vertexCache != null) {
            final Object field = vertex.getProperty(keyFieldName);
            if (field != null)
              vertexCache.remove(field);
          }

          g.removeVertex(vertex);

          verticesRemoved.incrementAndGet();
        } finally {
          g.shutdown();
          endAsynchOperation();
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

  public Edge addEdgeByVerticesKeys(final Object iOutVertex, final Object iInVertex, final String iEdgeLabel) {
    beginAsynchOperation();

    return new OrientEdgeFuture(Orient.instance().submit(new Callable<OrientEdge>() {
      @Override
      public OrientEdge call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          final OrientExtendedVertex vOut = getOrAddVertex(iOutVertex);
          final OrientExtendedVertex vIn = getOrAddVertex(iInVertex);

          for (int retry = 0;; retry++) {
            try {

              final OrientEdge e = g.addEdge(null, vOut, vIn, iEdgeLabel);

              addInCache(vOut.getProperty(keyFieldName), vOut.getVertexInstance());
              addInCache(vIn.getProperty(keyFieldName), vIn.getVertexInstance());

              edgesCreated.incrementAndGet();

              return e;

            } catch (OConcurrentModificationException e) {
              concurrentException.incrementAndGet();
              reloadVertices(vOut, vIn, iEdgeLabel, retry, e);
            } catch (ORecordDuplicatedException e) {
              indexUniqueException.incrementAndGet();
              reloadVertices(vOut, vIn, iEdgeLabel, retry, e);
            } catch (Exception e) {
              unknownException.incrementAndGet();
              OLogManager.instance().warn(
                  this,
                  "Error on addEdge(" + iOutVertex + "," + iInVertex + "," + iEdgeLabel + "), retrying (retry=" + retry + "/"
                      + maxRetries + ") Thread: " + Thread.currentThread().getId());
              e.printStackTrace();
            }
          }
        } finally {
          g.shutdown();
          endAsynchOperation();
        }
      }
    }));
  }

  @Override
  public Edge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
    beginAsynchOperation();

    return new OrientEdgeFuture(Orient.instance().submit(new Callable<OrientEdge>() {
      @Override
      public OrientEdge call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          // ASSURE ARE NOT REUSED FROM CACHE
          OrientVertex vOut = outVertex instanceof OrientVertexFuture ? ((OrientVertexFuture) outVertex).get()
              : (OrientVertex) outVertex;
          OrientVertex vIn = inVertex instanceof OrientVertexFuture ? ((OrientVertexFuture) inVertex).get()
              : (OrientVertex) inVertex;

          vOut.attach(g);
          vIn.attach(g);

          for (int retry = 0;; retry++) {
            try {

              final OrientEdge e = g.addEdge(id, vOut, vIn, label);

              addInCache(vOut.getProperty(keyFieldName), vOut);
              addInCache(vIn.getProperty(keyFieldName), vIn);

              edgesCreated.incrementAndGet();

              return e;

            } catch (OConcurrentModificationException e) {
              concurrentException.incrementAndGet();
              reloadVertices(vOut, vIn, label, retry, e);
            } catch (ORecordDuplicatedException e) {
              indexUniqueException.incrementAndGet();
              reloadVertices(vOut, vIn, label, retry, e);
            } catch (Exception e) {
              unknownException.incrementAndGet();
              OLogManager.instance().warn(
                  this,
                  "Error on addEdge(" + id + "," + outVertex + "," + inVertex + "," + label + "), retrying (retry=" + retry + "/"
                      + maxRetries + ") Thread: " + Thread.currentThread().getId());
              e.printStackTrace();
            }
          }
        } finally {
          g.shutdown();
          endAsynchOperation();
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
    beginAsynchOperation();

    Orient.instance().submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final OrientBaseGraph g = acquire();
        try {
          g.removeEdge(edge);
        } finally {
          g.shutdown();
          endAsynchOperation();
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

    dumpStats();

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
    init();
    final OrientBaseGraph g;
    if (transactional)
      g = factory.getTx();
    else
      g = factory.getNoTx();

    if (conflictStrategy != null) {
      final OStorage stg = g.getRawGraph().getStorage().getUnderlying();
      if (stg instanceof OAbstractPaginatedStorage)
        stg.setConflictStrategy(conflictStrategy);
    }

    return g;
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

  public ORecordConflictStrategy getConflictStrategy() {
    return conflictStrategy;
  }

  public OrientGraphAsynch setConflictStrategy(final String iStrategyName) {
    conflictStrategy = Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName);
    return this;
  }

  public OrientGraphAsynch setConflictStrategy(final ORecordConflictStrategy iResolver) {
    conflictStrategy = iResolver;
    return this;
  }

  protected void init() {
    if (factory == null) {
      synchronized (this) {
        if (factory == null)
          factory = new OrientGraphFactory(url, userName, userPassword).setupPool(1, maxPoolSize);
      }
    }
  }

  protected MERGE_RESULT mergeAndSaveRecord(final int retry, final ODocument existent, final Object[] prop)
      throws InterruptedException {
    // MERGE RECORD WITH INPUT CONTENT
    if (mergeRecords(existent, prop)) {
      try {
        existent.save();

      } catch (ONeedRetryException ex) {
        concurrentException.incrementAndGet();
        if (retry < maxRetries) {
          verticesReloaded.incrementAndGet();
          existent.reload(null, true);
          return MERGE_RESULT.RETRY;
        } else
          return MERGE_RESULT.ERROR;
      }
    }

    return MERGE_RESULT.MERGED;
  }

  protected void reloadVertices(final OrientExtendedVertex vOut, final OrientExtendedVertex vIn, final String iLabel,
      final int retry, final OException e) {
    if (retry < maxRetries) {
      if (OLogManager.instance().isDebugEnabled())
        OLogManager.instance().debug(this, "Conflict on addEdge(%s,%s,%s), retrying (retry=%d/%d). Cause: %s. Thread: %d);", e,
            vOut, vIn, iLabel, retry, maxRetries, e, Thread.currentThread().getId());

      if (e instanceof OConcurrentModificationException) {
        // LOAD ONLY THE CONFLICTED RECORD ONLY
        if (((OConcurrentModificationException) e).getRid().equals(vOut.getIdentity()))
          vOut.reload();
        else
          vIn.reload();

        verticesReloaded.incrementAndGet();
      } else {
        // LOAD BOTH VERTICES'S RECORD
        if (!vOut.getRecord().getIdentity().isNew()) {
          vOut.reload();
          verticesReloaded.incrementAndGet();
          // } else {
          // v = getVertex( ((ORecordDuplicatedException)e).getRid() )
          // vOut.setProperty("@rid");
        }
        if (!vIn.getRecord().getIdentity().isNew()) {
          vIn.reload();
          verticesReloaded.incrementAndGet();
        }
      }

    } else
      throw e;
  }

  protected void dumpStats() {
    if (outStats == null)
      return;

    outStats.printf("\n---------------------------------");
    outStats.printf("\nOrientGraphAsynch stats");
    outStats.printf("\n---------------------------------");

    outStats.printf("\npool instances used.: %d", factory.getAvailableInstancesInPool());
    outStats.printf("\nverticesCreated.....: %d", verticesCreated.get());
    outStats.printf("\nedgesCreated........: %d", edgesCreated.get());
    outStats.printf("\nverticesRemoved.....: %d", verticesRemoved.get());
    outStats.printf("\nverticesLoaded......: %d", verticesLoaded.get());
    outStats.printf("\nverticesReloaded....: %d", verticesReloaded.get());
    outStats.printf("\nreusedCachedVertex..: %d", reusedCachedVertex.get());
    outStats.printf("\nindexUniqueException: %d", indexUniqueException.get());
    outStats.printf("\nconcurrentException.: %d", concurrentException.get());
    outStats.printf("\nunknownException....: %d", unknownException.get());
    outStats.println("\n");

    verticesCreated.set(0);
    edgesCreated.set(0);
    verticesLoaded.set(0);
    reusedCachedVertex.set(0);
    indexUniqueException.set(0);
    concurrentException.set(0);
    unknownException.set(0);
  }

  protected boolean mergeRecords(final ODocument iSource, final Object[] prop) throws InterruptedException {
    boolean modified = false;
    for (int i = 0; i < prop.length; i += 2) {
      final String pName = prop[i].toString();
      final Object pValue = prop[i + 1];

      final Object fieldValue = iSource.field(pName);
      if (fieldValue == null || (!fieldValue.equals(pValue))) {
        // OVERWRITE PROPERTY VALUE
        iSource.field(pName, fieldValue);
        modified = true;
      }
    }
    return modified;
  }

  protected OrientVertex getFromCache(final Object id) {
    if (vertexCache != null) {
      final OrientVertex v = vertexCache.remove(id);
      if (v != null) {
        reusedCachedVertex.incrementAndGet();
        // System.out.printf("\nThread " + Thread.currentThread().getId() + " Reused from cache key=%s v=%s", id, v);
        return v;
      }
    }

    return null;
  }

  protected void addInCache(final Object id, final OrientVertex v) {
    if (vertexCache != null && id != null && v != null) {
      if (v.getRecord().getIdentity().isNew()) {
        v.getRecord().setDirty();
        v.save();
      }

      if (!v.getRecord().getIdentity().isPersistent())
        OLogManager.instance().warn(this, "Cannot put a non persistent object in cache, key=%s, vertex=%s", id, v);

      vertexCache.put(id, v);
      // System.out.printf("\nPut in cache key=%s v=%s", id, v);
    }
  }

  protected void beginAsynchOperation() {
    init();
    operationStarted.incrementAndGet();
  }

  protected void endAsynchOperation() {
    operationCompleted.incrementAndGet();
    synchronized (operationCompleted) {
      operationCompleted.notifyAll();
    }
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
    FEATURES.supportsThreadIsolatedTransactions = false;
  }
}
