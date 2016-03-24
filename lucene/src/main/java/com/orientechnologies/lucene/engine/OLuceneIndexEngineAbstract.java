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
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.lucene.tx.OLuceneTxChangesMultiRid;
import com.orientechnologies.lucene.tx.OLuceneTxChangesSingleRid;
import com.orientechnologies.orient.core.OOrientListener;
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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.INDEX;
import static com.orientechnologies.lucene.analyzer.OLuceneAnalyzerFactory.AnalyzerKind.QUERY;

public abstract class OLuceneIndexEngineAbstract<V> extends OSharedResourceAdaptiveExternal
    implements OLuceneIndexEngine, OOrientListener {

  public static final String               RID              = "RID";
  public static final String               KEY              = "KEY";
  public static final String               STORED           = "_STORED";

  public static final String               OLUCENE_BASE_DIR = "luceneIndexes";

  protected SearcherManager                searcherManager;
  protected OIndexDefinition               index;
  protected TrackingIndexWriter            mgrWriter;
  protected String                         name;
  protected String                         clusterIndexName;
  protected boolean                        automatic;
  protected ControlledRealTimeReopenThread nrt;
  protected ODocument                      metadata;
  protected Version                        version;
  protected Map<String, Boolean>           collectionFields = new HashMap<String, Boolean>();
  protected TimerTask                      commitTask;
  protected AtomicBoolean                  closed           = new AtomicBoolean(true);
  private long                             reopenToken;
  private Analyzer                         indexAnalyzer;
  private Analyzer                         queryAnalyzer;
  private Directory                        directory;

  public OLuceneIndexEngineAbstract(String indexName) {
    super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean(),
        OGlobalConfiguration.MVRBTREE_TIMEOUT.getValueAsInteger(), true);
    this.name = indexName;

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

  protected abstract IndexWriter openIndexWriter(Directory directory) throws IOException;

  protected void addDocument(Document doc) {
    try {

      reopenToken = mgrWriter.addDocument(doc);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on adding new document '%s' to Lucene index", e, doc);
    }
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, ODocument metadata) {

  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    // FIXME how many timers are around?
    Orient.instance().registerListener(this);
    commitTask = new TimerTask() {
      @Override
      public void run() {
        if (Boolean.FALSE.equals(closed.get())) {
          commit();
        }
      }
    };
    Orient.instance().scheduleTask(commitTask, 10000, 10000);

    this.index = indexDefinition;
    this.automatic = isAutomatic;
    this.metadata = metadata;

    OLuceneAnalyzerFactory fc = new OLuceneAnalyzerFactory();
    indexAnalyzer = fc.createAnalyzer(indexDefinition, INDEX, metadata);
    queryAnalyzer = fc.createAnalyzer(indexDefinition, QUERY, metadata);

    try {

      this.index = indexDefinition;

      checkCollectionIndex(indexDefinition);
      reOpen(metadata);

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on initializing Lucene index", e);
    }
  }

  protected void commit() {
    try {
      mgrWriter.getIndexWriter().commit();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on committing Lucene index", e);
    }
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

  private void reOpen(final ODocument metadata) throws IOException {

    OLuceneDirectoryFactory directoryFactory = new OLuceneDirectoryFactory();

    directory = directoryFactory.createDirectory(getDatabase(), name, metadata);
    final IndexWriter indexWriter = createIndexWriter(directory);
    mgrWriter = new TrackingIndexWriter(indexWriter);
    searcherManager = new SearcherManager(indexWriter, true, null);

    if (nrt != null) {
      nrt.close();
    }

    nrt = new ControlledRealTimeReopenThread(mgrWriter, searcherManager, 60.00, 0.1);
    nrt.setDaemon(true);
    nrt.start();
    flush();
  }

  protected abstract IndexWriter createIndexWriter(Directory directory) throws IOException;

  @Override
  public void flush() {

    try {
      mgrWriter.getIndexWriter().commit();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on flushing Lucene index", e);
    }

  }

  @Override
  public void delete() {

    try {
      if (mgrWriter != null && mgrWriter.getIndexWriter() != null) {
        closeIndex();
      }

      if (directory instanceof FSDirectory) {
        Path path = ((FSDirectory) this.directory).getDirectory();
        OFileUtils.deleteRecursively(path.toFile());
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting Lucene index", e);
    }
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    internalDelete(indexName);
  }

  protected void internalDelete(String indexName) {
    try {
      if (mgrWriter != null && mgrWriter.getIndexWriter() != null) {
        closeIndex();
      }
      ODatabaseDocumentInternal database = getDatabase();
      final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) database.getStorage().getUnderlying();
      if (storageLocalAbstract instanceof OLocalPaginatedStorage) {

        OLocalPaginatedStorage localAbstract = (OLocalPaginatedStorage) storageLocalAbstract;

        File f = new File(getIndexPath(localAbstract, indexName));
        OFileUtils.deleteRecursively(f);
        f = new File(getIndexBasePath(localAbstract));
        OFileUtils.deleteFolderIfEmpty(f);
      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on deleting Lucene index", e);
    }
  }

  protected void closeIndex() throws IOException {
    OLogManager.instance().debug(this, "Closing Lucene index '" + this.name + "'...");

    if (nrt != null) {
      nrt.interrupt();
      nrt.close();
    }
    if (commitTask != null) {
      commitTask.cancel();
    }

    if (searcherManager != null)
      searcherManager.close();

    if (mgrWriter != null) {
      mgrWriter.getIndexWriter().commit();
      mgrWriter.getIndexWriter().close();
    }
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  private String getIndexPath(OLocalPaginatedStorage storageLocalAbstract, String indexName) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR + File.separator + indexName;
  }

  protected String getIndexBasePath(OLocalPaginatedStorage storageLocalAbstract) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR;
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize) {
    // initIndex(indexName, indexDefinition, isAutomatic, metadata);
  }

  @Override
  public void clear() {
    try {
      reopenToken = mgrWriter.deleteAll();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on clearing Lucene index", e);
    }
  }

  @Override
  public void close() {
    try {
      closeIndex();
    } catch (Throwable e) {
      OLogManager.instance().error(this, "Error on closing Lucene index", e);
    }
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer vValuesTransformer) {
    return null;
  }

  public long size(final ValuesTransformer transformer) {
    return sizeInTx(null);
  }

  protected void release(IndexSearcher searcher) {
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

  private String getIndexPath(OLocalPaginatedStorage storageLocalAbstract) {
    return getIndexPath(storageLocalAbstract, name);
  }

  @Override
  public String indexName() {
    return name;
  }

  public abstract void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret,
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

    Query query = deleteQuery(key, value);
    if (query != null)
      deleteDocument(query);
    return true;
  }

  protected void deleteDocument(Query query) {
    try {
      reopenToken = mgrWriter.deleteDocuments(query);
      if (!mgrWriter.getIndexWriter().hasDeletions()) {
        OLogManager.instance().error(this, "Error on deleting document by query '%s' to Lucene index",
            new OIndexException("Error deleting document"), query);
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

  @Override
  public IndexSearcher searcher() throws IOException {
    try {
      nrt.waitForGeneration(reopenToken);
      return searcherManager.acquire();
    } catch (InterruptedException e) {
      OLogManager.instance().error(this, "Error on get searcher from Lucene index", e);
    }
    return null;

  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    IndexReader reader = null;
    IndexSearcher searcher = null;
    try {
      searcher = searcher();
      if (searcher != null)
        reader = searcher.getIndexReader();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on getting size of Lucene index", e);
    } finally {
      if (searcher != null) {
        release(searcher);
      }
    }
    return changes == null ? reader.numDocs() : reader.numDocs() + changes.numDocs();
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    if (isCollectionDelete()) {
      return new OLuceneTxChangesMultiRid(this, createIndexWriter(new RAMDirectory()));
    } else {
      return new OLuceneTxChangesSingleRid(this, createIndexWriter(new RAMDirectory()));
    }
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    if (isCollectionDelete()) {
      return OLuceneIndexType.createDeleteQuery(value, index.getFields(), key);
    }
    return OLuceneIndexType.createQueryId(value);
  }

  protected Field.Store isToStore(String f) {
    return collectionFields.get(f) ? Field.Store.YES : Field.Store.NO;
  }

  @Override
  public void onShutdown() {
    close();
  }

  @Override
  public void onStorageRegistered(OStorage storage) {

  }

  @Override
  public void onStorageUnregistered(OStorage storage) {

  }
}
