package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.builder.ODocBuilder;
import com.orientechnologies.lucene.builder.OQueryBuilderImpl;
import com.orientechnologies.lucene.collections.LuceneResultSetFactory;
import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.collections.OLuceneAbstractResultSet;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.lucene.tx.OLuceneTxChangesSingleRid;
import com.orientechnologies.orient.core.OOrientListener;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.storage.OStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by frank on 10/8/15.
 */
public class OLuceneFullTextExpIndexEngine implements OLuceneIndexEngine, OOrientListener {

  public static final String RID    = "RID";
  public static final String KEY    = "KEY";
  public static final String STORED = "_STORED";

  private final String         name;
  private final OLuceneStorage luceneStorage;

  private ODocBuilder       docBuilder;
  private OQueryBuilderImpl queryBuilder;

  private String                   indexName;
  private String                   indexType;
  private OIndexDefinition         indexDefinition;
  private ODocument                indexMetadata;
  private boolean                  isAutomatic;
  private OLuceneClassIndexContext indexContext;

  public OLuceneFullTextExpIndexEngine(String name, OLuceneStorage luceneStorage, ODocBuilder oDocBuilder,
      OQueryBuilderImpl oQueryBuilder) {
    this.name = name;
    this.luceneStorage = luceneStorage;
    this.docBuilder = oDocBuilder;
    this.queryBuilder = oQueryBuilder;
  }

  @Override
  public void initIndex(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic,
      ODocument indexMetadata) {
    this.indexName = indexName;
    this.indexType = indexType;
    this.indexDefinition = indexDefinition;
    this.isAutomatic = isAutomatic;
    this.indexMetadata = indexMetadata;

    indexContext = new OLuceneClassIndexContext(indexDefinition, indexName, isAutomatic, indexMetadata);

    luceneStorage.initIndex(indexContext);
  }

  @Override
  public String indexName() {
    return indexName;
  }

  protected ODatabaseDocumentInternal getDatabase() {
    return ODatabaseRecordThreadLocal.INSTANCE.get();
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return docBuilder.build(indexContext.definition, key, value, indexContext.fieldsToStore, indexContext.metadata);
  }

  @Override
  public Query buildQuery(Object query) {
    try {
      return queryBuilder.query(indexDefinition, query, luceneStorage.queryAnalyzer());
    } catch (ParseException e) {

      throw OException.wrapException(new OIndexEngineException("Error parsing query"), e);
    }
  }

  @Override
  public Analyzer queryAnalyzer() {

    return luceneStorage.queryAnalyzer();
  }

  @Override
  public Analyzer indexAnalyzer() {

    return luceneStorage.indexAnalyzer();
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    return luceneStorage.remove(key, value);
  }

  @Override
  public IndexSearcher searcher() throws IOException {
    return luceneStorage.searcher();
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    OLogManager.instance().info(this, "getInTx");

    Query q = null;
    try {
      q = queryBuilder.query(indexDefinition, key, luceneStorage.queryAnalyzer());
      OCommandContext context = null;
      if (key instanceof OFullTextCompositeKey) {
        context = ((OFullTextCompositeKey) key).getContext();
      }
      return getResults(q, context, key, changes);
    } catch (ParseException e) {
      throw OException.wrapException(new OIndexEngineException("Error parsing lucene query"), e);
    }

  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context, Object key, OLuceneTxChanges changes) {
    OLogManager.instance().info(this, "getResults:: " + query);

    try {
      IndexSearcher searcher = searcher();
      QueryContext queryContext = new QueryContext(context, searcher, query).setChanges(changes);
      //      if (facetManager.supportsFacets()) {
      //        facetManager.addFacetContext(queryContext, key);
      //      }
      OLuceneAbstractResultSet resultSet = LuceneResultSetFactory.INSTANCE.create(this, queryContext);

      OLogManager.instance().info(this, "getResults (restul Set) :: " + resultSet.size());

      return resultSet;
    } catch (IOException e) {
      throw OIOException.wrapException(new OIndexException("Error reading from Lucene index"), e);
    }

  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    return luceneStorage.sizeInTx(changes);
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {

    OLogManager.instance().info(this, "buildTxChanges");
    return new OLuceneTxChangesSingleRid(this, luceneStorage.createIndexWriter(new RAMDirectory()));

  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    return luceneStorage.deleteQuery(key, value);
  }

  @Override
  public void init() {
    luceneStorage.init();
  }

  @Override
  public void flush() {
    luceneStorage.flush();
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize) {
    luceneStorage.create(valueSerializer, isAutomatic, keyTypes, nullPointerSupport, keySerializer, keySize);
  }

  @Override
  public void delete() {
    // NOOP
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    luceneStorage.deleteWithoutLoad(indexName);
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize) {
    luceneStorage.load(indexName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullPointerSupport, keySize);
  }

  @Override
  public boolean contains(Object key) {
    return luceneStorage.contains(key);
  }

  @Override
  public boolean remove(Object key) {
    return luceneStorage.remove(key);
  }

  @Override
  public void clear() {
    luceneStorage.clear();
  }

  @Override
  public void close() {

    OLogManager.instance().info(this, "CLOSE ::: proxy close");
    luceneStorage.close();
  }

  @Override
  public Object get(Object key) {
    OLogManager.instance().info(this, "get");
    Object inTx = getInTx(key, null);

    return inTx;
  }

  @Override
  public void put(Object key, Object value) {

    Collection<OIdentifiable> container = (Collection<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType
          .createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      doc.add(OLuceneIndexType
          .createField("CLUSTER", oIdentifiable.getIdentity().getClusterId(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      doc.add(OLuceneIndexType
          .createField("CLASS", indexContext.indexClass.getName(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));

      int i = 0;
      if (indexDefinition.isAutomatic()) {
        mapFields(key, doc, i);
      }
      luceneStorage.addDocument(doc);

    }
  }

  private void putInManualindex(Object key, Document doc) {
    Object val = null;
    if (key instanceof OCompositeKey) {
      List<Object> keys = ((OCompositeKey) key).getKeys();

      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, val, Field.Store.NO, Field.Index.ANALYZED));
      }
    } else if (key instanceof Collection) {
      Collection<Object> keys = (Collection<Object>) key;
      int k = 0;
      for (Object o : keys) {
        doc.add(OLuceneIndexType.createField("k" + k, o, Field.Store.NO, Field.Index.ANALYZED));
      }
    } else {
      val = key;
      doc.add(OLuceneIndexType.createField("k0", val, Field.Store.NO, Field.Index.ANALYZED));
    }
  }

  private void mapFields(Object key, Document doc, int i) {

    for (String field : indexDefinition.getFields()) {
      Object val = null;
      if (key instanceof OCompositeKey) {
        val = ((OCompositeKey) key).getKeys().get(i);
        i++;
      } else {
        val = key;
      }

      if (val != null) {

        //FIXME why 2 fields? May we use STORED+ANALYZED?
        if (isToStore(field).equals(Field.Store.YES)) {
          doc.add(OLuceneIndexType.createField(indexContext.indexClass.getName() + "." + field + STORED, val, Field.Store.YES,
              Field.Index.NOT_ANALYZED_NO_NORMS));
        }
        doc.add(OLuceneIndexType
            .createField(indexContext.indexClass.getName() + "." + field, val, Field.Store.NO, Field.Index.ANALYZED));
      }
    }

  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret,
      final ScoreDoc score) {
    recordId.setContext(new HashMap<String, Object>() {
      {
        put("score", score.score);
      }
    });
  }

  protected Field.Store isToStore(String f) {
    return indexContext.fieldsToStore.get(f) ? Field.Store.YES : Field.Store.NO;
  }

  @Override
  public Object getFirstKey() {
    return luceneStorage.getFirstKey();
  }

  @Override
  public Object getLastKey() {
    return luceneStorage.getLastKey();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return null;
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return 0;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
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
  public void onShutdown() {
    OLogManager.instance().info(this, "SHUTDONW");
    luceneStorage.onShutdown();
  }

  @Override
  public void onStorageRegistered(OStorage storage) {

  }

  @Override
  public void onStorageUnregistered(OStorage storage) {

  }
}
