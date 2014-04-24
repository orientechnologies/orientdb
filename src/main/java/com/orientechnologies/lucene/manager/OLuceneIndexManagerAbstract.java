/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.manager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.lucene.utils.OLuceneIndexUtils;
import com.orientechnologies.orient.core.index.OIndexException;
import com.sun.jna.platform.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.OLuceneMapEntryIterator;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

public abstract class OLuceneIndexManagerAbstract<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {

  public static final String               RID            = "RID";
  public static final String               KEY            = "KEY";
  public static final Version              LUCENE_VERSION = Version.LUCENE_47;

  protected IndexWriter                    indexWriter    = null;
  protected SearcherManager                manager;
  protected OIndexDefinition               index;
  protected TrackingIndexWriter            mgrWriter;
  protected String                         indexName;
  protected String                         clusterIndexName;
  protected OStreamSerializer              serializer;
  protected boolean                        automatic;
  private OIndex                           managedIndex;
  protected ControlledRealTimeReopenThread nrt;
  private boolean                          rebuilding;
  private long                             reopenToken;
  protected ODocument                      metadata;

  protected Version                        version;

  public OLuceneIndexManagerAbstract() {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(), OGlobalConfiguration.MVRBTREE_TIMEOUT
        .getValueAsInteger(), true);
  }

  @Override
  public void create(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic) {

  }

  public void createIndex(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic, ODocument metadata) {
    initIndex(indexName, indexDefinition, clusterIndexName, valueSerializer, isAutomatic, metadata);
  }

  protected void initIndex(String indexName, OIndexDefinition indexDefinition, String clusterIndexName,
      OStreamSerializer valueSerializer, boolean isAutomatic, ODocument metadata) {
    this.index = indexDefinition;
    this.indexName = indexName;
    this.serializer = valueSerializer;
    this.automatic = isAutomatic;
    this.clusterIndexName = clusterIndexName;
    this.metadata = metadata;
    try {

      this.index = indexDefinition;

      reOpen(metadata);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void reOpen(ODocument metadata) throws IOException {
    ODatabaseRecord database = getDatabase();
    final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();
    Directory dir = NIOFSDirectory.open(new File(storageLocalAbstract.getStoragePath() + File.separator + indexName));
    indexWriter = createIndexWriter(dir, metadata);
    mgrWriter = new TrackingIndexWriter(indexWriter);
    manager = new SearcherManager(indexWriter, true, null);
    if (nrt != null) {
      nrt.close();
    }
    nrt = new ControlledRealTimeReopenThread(mgrWriter, manager, 60.00, 0.1);
    nrt.start();
  }

  protected IndexSearcher getSearcher() throws IOException {
    try {
      nrt.waitForGeneration(reopenToken);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return manager.acquire();
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  public abstract IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException;

  public abstract IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException;

  public void addDocument(Document doc) {
    try {
      reopenToken = mgrWriter.addDocument(doc);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void deleteDocument(Query query) {
    try {
      reopenToken = mgrWriter.deleteDocuments(query);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean remove(Object key, OIdentifiable value) {

    deleteDocument(OLuceneIndexType.createQueryId(value));
    return true;
  }

  public void commit() {
    try {
      indexWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void delete() {
    try {
      if (indexWriter != null) {
        indexWriter.deleteAll();
        indexWriter.close();
        indexWriter.getDirectory().close();
      }
      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();
      File f = new File(storageLocalAbstract.getStoragePath() + File.separator + indexName);

      OLuceneIndexUtils.deleteFolder(f);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public Iterator<Map.Entry<Object, V>> iterator() {
    try {
      IndexReader reader = getSearcher().getIndexReader();
      return new OLuceneMapEntryIterator<Object, V>(reader, index);

    } catch (IOException e) {
      e.printStackTrace();
    }
    return new HashSet<Map.Entry<Object, V>>().iterator();
  }

  public void clear() {
    try {
      indexWriter.deleteAll();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void startTransaction() {

  }

  @Override
  public void stopTransaction() {

  }

  @Override
  public void afterTxRollback() {

  }

  @Override
  public void afterTxCommit() {

  }

  @Override
  public void closeDb() {

  }

  @Override
  public void beforeTxBegin() {

  }

  @Override
  public void flush() {
    try {
      indexWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      nrt.close();
      indexWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void unload() {
    try {
      indexWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void rollback() {
    try {
      indexWriter.rollback();
      reOpen(metadata);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition,OStreamSerializer valueSerializer, boolean isAutomatic) {

  }

  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    initIndex(indexName, indexDefinition, null, null, isAutomatic, metadata);
  }

  public long size(ValuesTransformer<V> transformer) {
    IndexReader reader = null;
    try {
      reader = getSearcher().getIndexReader();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return reader.numDocs();
  }

  public void setManagedIndex(OIndex index) {
    this.managedIndex = index;
  }

  public void setRebuilding(boolean rebuilding) {
    this.rebuilding = rebuilding;
  }

  public boolean isRebuilding() {
    return rebuilding;
  }

  public Analyzer getAnalyzer(ODocument metadata) {
    Analyzer analyzer = null;
    if (metadata != null) {
      String analyzerString = metadata.field("analyzer");
      if (analyzerString != null) {
        try {
          Class classAnalyzer = Class.forName(analyzerString);
          Constructor constructor = classAnalyzer.getConstructor(Version.class);
          analyzer = (Analyzer) constructor.newInstance(getVersion(metadata));
        } catch (ClassNotFoundException e) {
          throw new OIndexException("Analyzer: " + analyzerString + " not found", e);
        } catch (NoSuchMethodException e) {
          e.printStackTrace();
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
      }
    } else {
      analyzer = new StandardAnalyzer(getVersion(metadata));
    }
    return analyzer;
  }

  public Version getVersion(ODocument metadata) {
    if (version == null) {
      version = LUCENE_VERSION;
    }
    return version;
  }
}
