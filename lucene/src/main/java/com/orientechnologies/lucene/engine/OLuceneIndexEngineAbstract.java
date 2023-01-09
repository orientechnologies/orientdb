/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.QUERY;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory;
import com.orientechnologies.lucene.builder.OLuceneIndexType;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.lucene.tx.OLuceneTxChangesMultiRid;
import com.orientechnologies.lucene.tx.OLuceneTxChangesSingleRid;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.ControlledRealTimeReopenThread;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public abstract class OLuceneIndexEngineAbstract extends OSharedResourceAdaptive
    implements OLuceneIndexEngine {

  public static final String RID = "RID";
  public static final String KEY = "KEY";

  private final AtomicLong lastAccess;
  private SearcherManager searcherManager;
  protected OIndexDefinition indexDefinition;
  protected String name;
  private ControlledRealTimeReopenThread<IndexSearcher> nrt;
  protected ODocument metadata;
  protected Version version;
  protected Map<String, Boolean> collectionFields = new HashMap<>();
  private TimerTask commitTask;
  private final AtomicBoolean closed;
  private final OStorage storage;
  private volatile long reopenToken;
  private Analyzer indexAnalyzer;
  private Analyzer queryAnalyzer;
  private volatile OLuceneDirectory directory;
  private IndexWriter indexWriter;
  private long flushIndexInterval;
  private long closeAfterInterval;
  private long firstFlushAfter;
  private final int id;

  public OLuceneIndexEngineAbstract(int id, OStorage storage, String name) {
    super(true, 0, true);
    this.id = id;

    this.storage = storage;
    this.name = name;

    lastAccess = new AtomicLong(System.currentTimeMillis());

    closed = new AtomicBoolean(true);
  }

  @Override
  public int getId() {
    return id;
  }

  protected void updateLastAccess() {
    lastAccess.set(System.currentTimeMillis());
  }

  protected void addDocument(Document doc) {
    try {

      reopenToken = indexWriter.addDocument(doc);
    } catch (IOException e) {
      OLogManager.instance()
          .error(this, "Error on adding new document '%s' to Lucene index", e, doc);
    }
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {

    this.indexDefinition = indexDefinition;
    this.metadata = metadata;

    OLuceneAnalyzerFactory fc = new OLuceneAnalyzerFactory();
    indexAnalyzer = fc.createAnalyzer(indexDefinition, INDEX, metadata);
    queryAnalyzer = fc.createAnalyzer(indexDefinition, QUERY, metadata);

    checkCollectionIndex(indexDefinition);

    flushIndexInterval =
        Optional.ofNullable(metadata.<Integer>getProperty("flushIndexInterval"))
            .orElse(10000)
            .longValue();

    closeAfterInterval =
        Optional.ofNullable(metadata.<Integer>getProperty("closeAfterInterval"))
            .orElse(120000)
            .longValue();

    firstFlushAfter =
        Optional.ofNullable(metadata.<Integer>getProperty("firstFlushAfter"))
            .orElse(10000)
            .longValue();
  }

  private void scheduleCommitTask() {
    commitTask =
        Orient.instance()
            .scheduleTask(
                () -> {
                  if (shouldClose()) {
                    synchronized (OLuceneIndexEngineAbstract.this) {
                      // while on lock the index was opened
                      if (!shouldClose()) return;
                      doClose(false);
                    }
                  }
                  if (!closed.get()) {

                    OLogManager.instance().debug(this, "Flushing index: " + indexName());
                    flush();
                  }
                },
                firstFlushAfter,
                flushIndexInterval);
  }

  private boolean shouldClose() {
    //noinspection resource
    return !(directory.getDirectory() instanceof RAMDirectory)
        && System.currentTimeMillis() - lastAccess.get() > closeAfterInterval;
  }

  private void checkCollectionIndex(OIndexDefinition indexDefinition) {

    List<String> fields = indexDefinition.getFields();

    OClass aClass =
        getDatabase().getMetadata().getSchema().getClass(indexDefinition.getClassName());
    for (String field : fields) {
      OProperty property = aClass.getProperty(field);

      if (property.getType().isEmbedded() && property.getLinkedType() != null) {
        collectionFields.put(field, true);
      } else {
        collectionFields.put(field, false);
      }
    }
  }

  private void reOpen(OStorage storage) throws IOException {
    //noinspection resource
    if (indexWriter != null
        && indexWriter.isOpen()
        && directory.getDirectory() instanceof RAMDirectory) {
      // don't waste time reopening an in memory index
      return;
    }
    open(storage);
  }

  protected static ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.instance().get();
  }

  private synchronized void open(OStorage storage) throws IOException {

    if (!closed.get()) return;

    OLuceneDirectoryFactory directoryFactory = new OLuceneDirectoryFactory();

    directory = directoryFactory.createDirectory(storage, name, metadata);

    indexWriter = createIndexWriter(directory.getDirectory());
    searcherManager = new SearcherManager(indexWriter, true, true, null);

    reopenToken = 0;

    startNRT();

    closed.set(false);

    flush();

    scheduleCommitTask();

    addMetadataDocumentIfNotPresent();
  }

  private void addMetadataDocumentIfNotPresent() {

    final IndexSearcher searcher = searcher();

    try {
      final TopDocs topDocs =
          searcher.search(new TermQuery(new Term("_CLASS", "JSON_METADATA")), 1);
      if (topDocs.totalHits == 0) {
        String metaAsJson = metadata.toJSON();
        String defAsJson = indexDefinition.toStream().toJSON();
        Document metaDoc = new Document();
        metaDoc.add(new StringField("_META_JSON", metaAsJson, Field.Store.YES));
        metaDoc.add(new StringField("_DEF_JSON", defAsJson, Field.Store.YES));
        metaDoc.add(
            new StringField(
                "_DEF_CLASS_NAME", indexDefinition.getClass().getCanonicalName(), Field.Store.YES));
        metaDoc.add(new StringField("_CLASS", "JSON_METADATA", Field.Store.YES));
        addDocument(metaDoc);
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error while retrieving index metadata", e);
    } finally {
      release(searcher);
    }
  }

  private void startNRT() {
    nrt = new ControlledRealTimeReopenThread<>(indexWriter, searcherManager, 60.00, 0.1);
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
    if (indexWriter != null && indexWriter.isOpen()) {
      indexWriter.commit();
      indexWriter.close();
      closed.set(true);
    }
  }

  protected abstract IndexWriter createIndexWriter(Directory directory) throws IOException;

  @Override
  public synchronized void flush() {
    try {
      if (!closed.get() && indexWriter != null && indexWriter.isOpen()) indexWriter.commit();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    }
  }

  @Override
  public void create(
      OAtomicOperation atomicOperation,
      OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OType[] keyTypes,
      boolean nullPointerSupport,
      OBinarySerializer keySerializer,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption) {}

  @Override
  public void delete(OAtomicOperation atomicOperation) {
    try {
      updateLastAccess();
      openIfClosed(storage);

      if (indexWriter != null && indexWriter.isOpen()) {
        synchronized (this) {
          doClose(true);
        }
      }

      final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) storage;
      if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
        OLocalPaginatedStorage localStorage = (OLocalPaginatedStorage) storageLocalAbstract;
        File storagePath = localStorage.getStoragePath().toFile();
        deleteIndexFolder(storagePath);
      }
    } catch (IOException e) {
      throw OException.wrapException(
          new OStorageException("Error during deletion of Lucene index " + name), e);
    }
  }

  private void deleteIndexFolder(File baseStoragePath) throws IOException {
    @SuppressWarnings("resource")
    final String[] files = directory.getDirectory().listAll();
    for (String fileName : files) {
      //noinspection resource
      directory.getDirectory().deleteFile(fileName);
    }
    directory.getDirectory().close();
    String indexPath = directory.getPath();
    if (indexPath != null) {
      File indexDir = new File(indexPath);
      final FileSystem fileSystem = FileSystems.getDefault();
      while (true) {
        if (Files.isSameFile(
            fileSystem.getPath(baseStoragePath.getCanonicalPath()),
            fileSystem.getPath(indexDir.getCanonicalPath()))) {
          break;
        }
        // delete only if dir is empty, otherwise stop deleting process
        // last index will remove all upper dirs
        final File[] indexDirFiles = indexDir.listFiles();
        if (indexDirFiles != null && indexDirFiles.length == 0) {
          OFileUtils.deleteRecursively(indexDir, true);
          indexDir = indexDir.getParentFile();
        } else {
          break;
        }
      }
    }
  }

  @Override
  public String indexName() {
    return name;
  }

  @Override
  public abstract void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret, ScoreDoc score);

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
    if (query != null) deleteDocument(query);
    return true;
  }

  @Override
  public boolean remove(Object key) {
    updateLastAccess();
    openIfClosed();
    try {
      final Query query = new QueryParser("", queryAnalyzer()).parse((String) key);
      deleteDocument(query);
      return true;
    } catch (org.apache.lucene.queryparser.classic.ParseException e) {
      OLogManager.instance().error(this, "Lucene parsing exception", e);
    }
    return false;
  }

  void deleteDocument(Query query) {
    try {
      reopenToken = indexWriter.deleteDocuments(query);
      if (!indexWriter.hasDeletions()) {
        OLogManager.instance()
            .error(
                this,
                "Error on deleting document by query '%s' to Lucene index",
                new OIndexException("Error deleting document"),
                query);
      }
    } catch (IOException e) {
      OLogManager.instance()
          .error(this, "Error on deleting document by query '%s' to Lucene index", e, query);
    }
  }

  private boolean isCollectionDelete() {
    boolean collectionDelete = false;
    for (Boolean aBoolean : collectionFields.values()) {
      collectionDelete = collectionDelete || aBoolean;
    }
    return collectionDelete;
  }

  protected synchronized void openIfClosed(OStorage storage) {
    if (closed.get()) {
      try {
        reOpen(storage);
      } catch (final IOException e) {
        OLogManager.instance().error(this, "error while opening closed index:: " + indexName(), e);
      }
    }
  }

  protected void openIfClosed() {
    openIfClosed(getDatabase().getStorage());
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
      return searcherManager.acquire();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on get searcher from Lucene index", e);
      throw OException.wrapException(
          new OLuceneIndexException("Error on get searcher from Lucene index"), e);
    }
  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    IndexSearcher searcher = searcher();
    try {
      @SuppressWarnings("resource")
      IndexReader reader = searcher.getIndexReader();

      // we subtract metadata document added during open
      return changes == null ? reader.numDocs() - 1 : reader.numDocs() + changes.numDocs() - 1;
    } finally {

      release(searcher);
    }
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    if (isCollectionDelete()) {
      return new OLuceneTxChangesMultiRid(
          this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    } else {
      return new OLuceneTxChangesSingleRid(
          this, createIndexWriter(new RAMDirectory()), createIndexWriter(new RAMDirectory()));
    }
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    updateLastAccess();
    openIfClosed();

    if (value == null || isCollectionDelete()) {
      return OLuceneIndexType.createDeleteQuery(value, indexDefinition.getFields(), key);
    }

    return OLuceneIndexType.createQueryId(value);
  }

  private void internalDelete() throws IOException {
    if (indexWriter != null && indexWriter.isOpen()) {
      close();
    }

    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) storage;
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      OLocalPaginatedStorage localPaginatedStorage = (OLocalPaginatedStorage) storageLocalAbstract;
      deleteIndexFolder(localPaginatedStorage.getStoragePath().toFile());
    }
  }

  @Override
  public void load(
      String indexName,
      OBinarySerializer valueSerializer,
      boolean isAutomatic,
      OBinarySerializer keySerializer,
      OType[] keyTypes,
      boolean nullPointerSupport,
      int keySize,
      Map<String, String> engineProperties,
      OEncryption encryption) {}

  @Override
  public void clear(OAtomicOperation atomicOperation) {
    updateLastAccess();
    openIfClosed();
    try {
      reopenToken = indexWriter.deleteAll();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on clearing Lucene index", e);
    }
  }

  @Override
  public synchronized void close() {
    doClose(false);
  }

  private void doClose(boolean onDelete) {
    if (closed.get()) return;

    try {
      cancelCommitTask();

      closeNRT();

      closeSearchManager();

      commitAndCloseWriter();

      if (!onDelete) directory.getDirectory().close();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on closing Lucene index", e);
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  @Override
  public Stream<Object> keyStream() {
    throw new UnsupportedOperationException("Cannot iterate over a lucene index");
  }

  public long size(final ValuesTransformer transformer) {
    return sizeInTx(null);
  }

  @Override
  public void release(IndexSearcher searcher) {
    updateLastAccess();
    openIfClosed();

    try {
      searcherManager.release(searcher);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on releasing index searcher  of Lucene index", e);
    }
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

  @Override
  public synchronized void freeze(boolean throwException) {
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
      reOpen(getDatabase().getStorage());
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on releasing Lucene index:: " + indexName(), e);
    }
  }
}
