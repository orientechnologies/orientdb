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

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.OLuceneMapEntryIterator;
import com.orientechnologies.lucene.utils.OLuceneIndexUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
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
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public abstract class OLuceneIndexManagerAbstract<V> extends OSharedResourceAdaptiveExternal implements OIndexEngine<V> {

  public static final String               RID              = "RID";
  public static final String               KEY              = "KEY";
  public static final Version              LUCENE_VERSION   = Version.LUCENE_47;

  public static final String               OLUCENE_BASE_DIR = "luceneIndexes";
  protected SearcherManager                searcherManager;
  protected OIndexDefinition               index;
  protected TrackingIndexWriter            mgrWriter;
  protected String                         indexName;
  protected String                         clusterIndexName;
  protected OStreamSerializer              serializer;
  protected boolean                        automatic;
  protected ControlledRealTimeReopenThread nrt;
  protected ODocument                      metadata;
  protected Version                        version;
  private OIndex                           managedIndex;
  private boolean                          rebuilding;
  private long                             reopenToken;

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

  public abstract IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException;

  public abstract IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException;

  public void addDocument(Document doc) {
    try {
      reopenToken = mgrWriter.addDocument(doc);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on adding new document '%s' to Lucene index", e, doc);
    }
  }

  public void deleteDocument(Query query) {
    try {
      reopenToken = mgrWriter.deleteDocuments(query);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting document by query '%s' to Lucene index", e, query);
    }
  }

  public boolean remove(Object key, OIdentifiable value) {
    deleteDocument(OLuceneIndexType.createQueryId(value));
    return true;
  }

  public void commit() {
    try {
      mgrWriter.getIndexWriter().commit();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on committing Lucene index", e);
    }
  }

  public void delete() {
    try {
      if (mgrWriter.getIndexWriter() != null) {
        closeIndex();
      }
      ODatabaseRecord database = getDatabase();
      final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();
      File f = new File(getIndexPath(storageLocalAbstract));

      OLuceneIndexUtils.deleteFolder(f);

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting Lucene index", e);
    }
  }

  public Iterator<Map.Entry<Object, V>> iterator() {
    try {
      IndexReader reader = getSearcher().getIndexReader();
      return new OLuceneMapEntryIterator<Object, V>(reader, index);

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on creating iterator against Lucene index", e);
    }
    return new HashSet<Map.Entry<Object, V>>().iterator();
  }

  public void clear() {
    try {
      mgrWriter.getIndexWriter().deleteAll();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on clearing Lucene index", e);
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
      mgrWriter.getIndexWriter().commit();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    }
  }

  @Override
  public void close() {
    try {
      closeIndex();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on closing Lucene index", e);
    }
  }

  @Override
  public void unload() {
    try {
      mgrWriter.getIndexWriter().commit();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on unloading Lucene index", e);
    }
  }

  public void rollback() {
    try {
      mgrWriter.getIndexWriter().rollback();
      reOpen(metadata);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on rolling back Lucene index", e);
    }
  }

  @Override
  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, OStreamSerializer valueSerializer,
      boolean isAutomatic) {

  }

  public void load(ORID indexRid, String indexName, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    initIndex(indexName, indexDefinition, null, null, isAutomatic, metadata);
  }

  public long size(ValuesTransformer<V> transformer) {
    IndexReader reader = null;
    try {
      reader = getSearcher().getIndexReader();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on getting size of Lucene index", e);
    }
    return reader.numDocs();
  }

  public void setManagedIndex(OIndex index) {
    this.managedIndex = index;
  }

  public boolean isRebuilding() {
    return rebuilding;
  }

  public void setRebuilding(boolean rebuilding) {
    this.rebuilding = rebuilding;
  }

  public Analyzer getAnalyzer(final ODocument metadata) {
    Analyzer analyzer = null;
    if (metadata != null && metadata.field("analyzer") != null) {
      final String analyzerString = metadata.field("analyzer");
      if (analyzerString != null) {
        try {

          final Class classAnalyzer = Class.forName(analyzerString);
          final Constructor constructor = classAnalyzer.getConstructor(Version.class);

          analyzer = (Analyzer) constructor.newInstance(getVersion(metadata));
        } catch (ClassNotFoundException e) {
          throw new OIndexException("Analyzer: " + analyzerString + " not found", e);
        } catch (NoSuchMethodException e) {
          Class classAnalyzer = null;
          try {
            classAnalyzer = Class.forName(analyzerString);
            analyzer = (Analyzer) classAnalyzer.newInstance();

          } catch (Throwable e1) {
            throw new OIndexException("Couldn't instantiate analyzer:  public constructor  not found", e1);
          }

        } catch (Exception e) {
          OLogManager.instance().error(this, "Error on getting analyzer for Lucene index", e);
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
      OLogManager.instance().error(this, "Error on initializing Lucene index", e);
    }
  }

  protected IndexSearcher getSearcher() throws IOException {
    try {
      nrt.waitForGeneration(reopenToken);
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error on get searcher from Lucene index", e);
    }
    return searcherManager.acquire();
  }

  protected void closeIndex() throws IOException {
    nrt.interrupt();
    nrt.close();
    searcherManager.close();
    mgrWriter.getIndexWriter().commit();
    mgrWriter.getIndexWriter().close();
  }

  private void reOpen(ODocument metadata) throws IOException {
    ODatabaseRecord database = getDatabase();
    final OStorageLocalAbstract storageLocalAbstract = (OStorageLocalAbstract) database.getStorage().getUnderlying();
    String pathname = getIndexPath(storageLocalAbstract);
    Directory dir = NIOFSDirectory.open(new File(pathname));
    IndexWriter indexWriter = createIndexWriter(dir, metadata);
    mgrWriter = new TrackingIndexWriter(indexWriter);
    searcherManager = new SearcherManager(indexWriter, true, null);
    if (nrt != null) {
      nrt.close();
    }
    nrt = new ControlledRealTimeReopenThread(mgrWriter, searcherManager, 60.00, 0.1);
    nrt.setDaemon(true);
    nrt.start();
  }

  private String getIndexPath(OStorageLocalAbstract storageLocalAbstract) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR + File.separator + indexName;
  }

  private ODatabaseRecord getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }


    @Override
    public OIndexCursor descCursor(ValuesTransformer<V> vValuesTransformer) {
    return null;
    }
}
