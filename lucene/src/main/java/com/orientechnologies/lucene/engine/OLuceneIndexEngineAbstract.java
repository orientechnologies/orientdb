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

package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptiveExternal;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.lucene.tx.OLuceneTxChangesMultiRid;
import com.orientechnologies.lucene.tx.OLuceneTxChangesSingleRid;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TrackingIndexWriter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.QUERY;

public abstract class OLuceneIndexEngineAbstract<V> extends OSharedResourceAdaptiveExternal implements OLuceneIndexEngine {

  public static final String RID    = "RID";
  public static final String KEY    = "KEY";
  public static final String STORED = "_STORED";

  public static final String OLUCENE_BASE_DIR = "luceneIndexes";

  protected final AtomicLong                     lastAccess;
  protected       SearcherManager                searcherManager;
  protected       OIndexDefinition               index;
  protected       String                         name;
  protected       String                         clusterIndexName;
  protected       boolean                        automatic;
  protected       ControlledRealTimeReopenThread nrt;
  protected       ODocument                      metadata;
  protected       Version                        version;
  protected Map<String, Boolean> collectionFields = new HashMap<String, Boolean>();
  protected TimerTask commitTask;
  protected AtomicBoolean closed = new AtomicBoolean(false);
  protected OStorage            storage;
  private   long                reopenToken;
  private   Analyzer            indexAnalyzer;
  private   Analyzer            queryAnalyzer;
  private   Directory           directory;
  private   TrackingIndexWriter mgrWriter;
  private   long                flushIndexInterval;
  private   long                closeAfterInterval;
  private   long                firstFlushAfter;

  private Lock openCloseLock;

  public OLuceneIndexEngineAbstract(OStorage storage, String indexName) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
        OGlobalConfiguration.MVRBTREE_TIMEOUT.getValueAsInteger(), true);

    this.storage = storage;
    this.name = indexName;

    lastAccess = new AtomicLong(System.currentTimeMillis());

    closed = new AtomicBoolean(true);

    openCloseLock = new ReentrantLock();
  }

  // TODO: move to utility class
  public static void sendTotalHits(String indexName, OCommandContext context, int totalHits) {
    if (context != null) {

      if (context.getVariable("totalHits") == null) {
        context.setVariable("totalHits", totalHits);
      } else {
        context.setVariable("totalHits", null);
      }
      context.setVariable((indexName + ".totalHits").replace(".", "_"), totalHits);
    }
  }

  // TODO: move to utility class
  public static void sendLookupTime(String indexName, OCommandContext context, final TopDocs docs, final Integer limit,
      long startFetching) {
    if (context != null) {

      final long finalTime = System.currentTimeMillis() - startFetching;
      context.setVariable((indexName + ".lookupTime").replace(".", "_"), new HashMap<String, Object>() {
        {
          put("limit", limit);
          put("totalTime", finalTime);
          put("totalHits", docs.totalHits);
          put("returnedHits", docs.scoreDocs.length);
          if (!Float.isNaN(docs.getMaxScore())) {
            put("maxScore", docs.getMaxScore());
          }

        }
      });
    }
  }

  protected void updateLastAccess() {
    lastAccess.set(System.currentTimeMillis());
  }

  protected abstract IndexWriter openIndexWriter(Directory directory) throws IOException;

  protected void addDocument(Document doc) {
    try {

      reopenToken = mgrWriter.addDocument(doc);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on adding new document '%s' to Lucene index", e, doc);
    }
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {

    this.index = indexDefinition;
    this.automatic = isAutomatic;
    this.metadata = metadata;

    OLuceneAnalyzerFactory fc = new OLuceneAnalyzerFactory();
    indexAnalyzer = fc.createAnalyzer(indexDefinition, INDEX, metadata);
    queryAnalyzer = fc.createAnalyzer(indexDefinition, QUERY, metadata);

    checkCollectionIndex(indexDefinition);

    if (metadata.containsField("flushIndexInterval")) {
      flushIndexInterval = Integer.valueOf(metadata.<Integer>field("flushIndexInterval")).longValue();
    } else {
      flushIndexInterval = 10000l;
    }

    if (metadata.containsField("closeAfterInterval")) {
      closeAfterInterval = Integer.valueOf(metadata.<Integer>field("closeAfterInterval")).longValue();
    } else {
      closeAfterInterval = 600000l;
    }

    if (metadata.containsField("firstFlushAfter")) {
      firstFlushAfter = Integer.valueOf(metadata.<Integer>field("firstFlushAfter")).longValue();
    } else {
      firstFlushAfter = 10000l;
    }

  }

  private void scheduleCommitTask() {
    commitTask = new TimerTask() {
      @Override
      public boolean cancel() {
//        OLogManager.instance().info(this, " Cancelling commit task for index:: " + indexName());
        return super.cancel();
      }

      @Override
      public void run() {

        if (shouldClose()) {
//          OLogManager.instance().info(this, " Closing index:: " + indexName());

          openCloseLock.lock();

          //while on lock the index was opened
          if (!shouldClose())
            return;
          try {

            close();
          } finally {
            openCloseLock.unlock();
          }

        }
        if (!closed.get()) {
//          OLogManager.instance().info(this, " Flushing index:: " + indexName());
          flush();
        }
      }
    };

    Orient.instance().scheduleTask(commitTask, firstFlushAfter, flushIndexInterval);
  }

  private boolean shouldClose() {
    return !(directory instanceof RAMDirectory) && System.currentTimeMillis() - lastAccess.get() > closeAfterInterval;
  }

  private void checkCollectionIndex(OIndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    OClass aClass = getDatabase().getMetadata().getSchema().getClass(indexDefinition.getClassName());
    for (String field : fields) {
      OProperty property = aClass.getProperty(field);

      if (property.getType().isEmbedded() && property.getLinkedType() != null) {
        collectionFields.put(field, true);
      } else {
        collectionFields.put(field, false);
      }
    }
  }

  protected void reOpen() throws IOException {

    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen() && directory instanceof RAMDirectory) {
      // don't waste time reopening an in memory index
      return;
    }
    open();
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private synchronized void open() throws IOException {

    if (!closed.get())
      return;

    openCloseLock.lock();

    try {
      OLuceneDirectoryFactory directoryFactory = new OLuceneDirectoryFactory();

      directory = directoryFactory.createDirectory(getDatabase(), name, metadata);

      final IndexWriter indexWriter = createIndexWriter(directory);
      mgrWriter = new TrackingIndexWriter(indexWriter);
      searcherManager = new SearcherManager(indexWriter, true, null);

      reopenToken = 0;

      startNRT();

      closed.set(false);

      flush();

      scheduleCommitTask();
    } finally {

      openCloseLock.unlock();
    }

  }

  private void startNRT() {
    nrt = new ControlledRealTimeReopenThread(mgrWriter, searcherManager, 60.00, 0.1);
    nrt.setDaemon(true);
    nrt.start();
  }

  private void closeNRT() {
    if (nrt != null) {
      nrt.interrupt();
      nrt.close();
    }
  }

  private void cancelCommitTask() {
    if (commitTask != null) {
      commitTask.cancel();
    }
  }

  private void closeSearchManager() throws IOException {
    if (searcherManager != null) {
      searcherManager.close();
    }
  }

  private void commitAndCloseWriter() throws IOException {
    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen()) {
      mgrWriter.getIndexWriter().commit();
      mgrWriter.getIndexWriter().close();
      closed.set(true);
    }
  }

  protected abstract IndexWriter createIndexWriter(Directory directory) throws IOException;

  @Override
  public void flush() {

    try {
      if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen())
        mgrWriter.getIndexWriter().commit();

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    }

  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata) {
  }

  @Override
  public void delete() {
    updateLastAccess();
    openIfClosed();

    if (mgrWriter != null && mgrWriter.getIndexWriter() != null) {

      try {
        mgrWriter.getIndexWriter().deleteUnusedFiles();
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during deletion of unused files", e);
      }
      close();
    }

    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) storage.getUnderlying();
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      OLocalPaginatedStorage localAbstract = (OLocalPaginatedStorage) storageLocalAbstract;
      deleteIndexFolder(indexName(), localAbstract);
    }

  }

  private void deleteIndexFolder(String indexName, OLocalPaginatedStorage localAbstract) {
    File f = new File(getIndexPath(localAbstract, indexName));
    OFileUtils.deleteRecursively(f);
    f = new File(getIndexBasePath(localAbstract));
    OFileUtils.deleteFolderIfEmpty(f);
  }

  @Override
  public String indexName() {
    return name;
  }

  private String getIndexPath(OLocalPaginatedStorage storageLocalAbstract, String indexName) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR + File.separator + indexName;
  }

  protected String getIndexBasePath(OLocalPaginatedStorage storageLocalAbstract) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR;
  }

  public abstract void onRecordAddedToResultSet(OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret,
      ScoreDoc score);

  @Override
  public Analyzer indexAnalyzer() {
    return indexAnalyzer;
  }

  @Override
  public Analyzer queryAnalyzer() {
    return queryAnalyzer;
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    updateLastAccess();
    openIfClosed();

    Query query = deleteQuery(key, value);
    if (query != null)
      deleteDocument(query);
    return true;
  }

  protected void deleteDocument(Query query) {
    try {
      reopenToken = mgrWriter.deleteDocuments(query);
      if (!mgrWriter.getIndexWriter().hasDeletions()) {
        OLogManager.instance()
            .error(this, "Error on deleting document by query '%s' to Lucene index", new OIndexException("Error deleting document"),
                query);
      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting document by query '%s' to Lucene index", e, query);
    }
  }

  protected boolean isCollectionDelete() {
    boolean collectionDelete = false;
    for (Boolean aBoolean : collectionFields.values()) {
      collectionDelete = collectionDelete || aBoolean;
    }
    return collectionDelete;
  }

  protected void openIfClosed() {
    if (closed.get()) {
//      OLogManager.instance().info(this, "open closed index:: " + indexName());

      try {
        reOpen();
      } catch (IOException e) {
        OLogManager.instance().error(this, "error while opening closed index:: " + indexName(), e);

      }
    }
  }

  @Override
  public boolean isCollectionIndex() {
    return isCollectionDelete();
  }

  @Override
  public IndexSearcher searcher() {
    try {
      updateLastAccess();
      openIfClosed();
      nrt.waitForGeneration(reopenToken);
      IndexSearcher searcher = searcherManager.acquire();
      return searcher;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on get searcher from Lucene index", e);
      throw OException.wrapException(new OLuceneIndexException("Error on get searcher from Lucene index"), e);
    }

  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    IndexSearcher searcher = searcher();
    try {
      IndexReader reader = searcher.getIndexReader();

      return changes == null ? reader.numDocs() : reader.numDocs() + changes.numDocs();
    } finally {

      release(searcher);
    }
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    if (isCollectionDelete()) {
      return new OLuceneTxChangesMultiRid(this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    } else {
      return new OLuceneTxChangesSingleRid(this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    }
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    updateLastAccess();
    openIfClosed();
    if (isCollectionDelete()) {
      return OLuceneIndexType.createDeleteQuery(value, index.getFields(), key);
    }
    return OLuceneIndexType.createQueryId(value);
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    internalDelete(indexName);
  }

  protected void internalDelete(String indexName) {
    if (mgrWriter != null && mgrWriter.getIndexWriter().isOpen()) {
      close();
    }

    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) storage.getUnderlying();
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      OLocalPaginatedStorage localAbstract = (OLocalPaginatedStorage) storageLocalAbstract;

      deleteIndexFolder(indexName, localAbstract);
    }
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties) {
    // initIndex(indexName, indexDefinition, isAutomatic, metadata);
  }

  @Override
  public void clear() {
    updateLastAccess();
    openIfClosed();
    try {
      reopenToken = mgrWriter.deleteAll();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on clearing Lucene index", e);
    }
  }

  @Override
  public synchronized void close() {
    if (closed.get())
      return;

    try {
//      OLogManager.instance().info(this, "Closing Lucene index '" + this.name + "'...");

      closeNRT();

      closeSearchManager();

      commitAndCloseWriter();

//      OLogManager.instance().info(this, "Closed Lucene index '" + this.name);
      cancelCommitTask();

    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on closing Lucene index", e);
    }
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  public long size(final ValuesTransformer transformer) {
    return sizeInTx(null);
  }

  protected void release(IndexSearcher searcher) {
    updateLastAccess();
    openIfClosed();
    try {
      searcherManager.release(searcher);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on releasing index searcher  of Lucene index", e);
    }
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return true; // do nothing
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return name;
  }

  private String getIndexPath(OLocalPaginatedStorage storageLocalAbstract) {
    return getIndexPath(storageLocalAbstract, name);
  }

  protected Field.Store isToStore(String f) {
    return collectionFields.get(f) ? Field.Store.YES : Field.Store.NO;
  }

  @Override
  public void freeze(boolean throwException) {

    try {
      closeNRT();
      cancelCommitTask();
      commitAndCloseWriter();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on freezing Lucene index:: " + indexName(), e);
    }

  }

  @Override
  public void release() {
    try {
      close();
      reOpen();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on releasing Lucene index:: " + indexName(), e);
    }
  }

  @Override
  public boolean isFrozen() {
    return closed.get();
  }
}
