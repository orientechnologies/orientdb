package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.analyzer.OLucenePerFieldAnalyzerWrapper;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.parser.OLuceneMultiFieldQueryParser;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.highlight.TextFragment;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.orientechnologies.lucene.OLuceneIndexFactory.LUCENE_ALGORITHM;

/**
 * Created by frank on 03/11/2016.
 */
public class OLuceneCrossClassIndexEngine implements OLuceneIndexEngine {

  private final OStorage   storage;
  private final String     indexName;
  private final AtomicLong bonsayFileId = new AtomicLong(0);
  private final int        indexId;

  public OLuceneCrossClassIndexEngine(int indexId, OStorage storage, String indexName) {
    this.indexId = indexId;

    this.storage = storage;
    this.indexName = indexName;

    OAbstractPaginatedStorage s = (OAbstractPaginatedStorage) storage;

  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {

  }

  @Override
  public void flush() {

  }

  @Override
  public int getId() {
    return indexId;
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
  }

  @Override
  public void delete() {

  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties, OEncryption encryption) {

  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public void close() {

  }

  @Override
  public Object get(Object key) {

    final OLuceneKeyAndMetadata keyAndMeta = (OLuceneKeyAndMetadata) key;
    final ODocument metadata = keyAndMeta.metadata;
    final List<String> excludes = Optional.ofNullable(metadata.<List<String>>getProperty("excludes"))
        .orElse(Collections.emptyList());
    final List<String> includes = Optional.ofNullable(metadata.<List<String>>getProperty("includes"))
        .orElse(Collections.emptyList());

    final Collection<? extends OIndex> indexes = ODatabaseRecordThreadLocal.instance().get().getMetadata().getIndexManager()
        .getIndexes().stream().filter(i -> !excludes.contains(i.getName()))
        .filter(i -> includes.isEmpty() || includes.contains(i.getName())).collect(Collectors.toList());

    final OLucenePerFieldAnalyzerWrapper globalAnalyzer = new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());

    final List<String> globalFields = new ArrayList<String>();

    final List<IndexReader> globalReaders = new ArrayList<IndexReader>();
    final Map<String, OType> types = new HashMap<>();

    try {
      for (OIndex index : indexes) {

        if (index.getAlgorithm().equalsIgnoreCase(LUCENE_ALGORITHM) && index.getType()
            .equalsIgnoreCase(OClass.INDEX_TYPE.FULLTEXT.toString())) {

          final OIndexDefinition definition = index.getDefinition();
          final String className = definition.getClassName();

          String[] indexFields = definition.getFields().toArray(new String[definition.getFields().size()]);

          for (int i = 0; i < indexFields.length; i++) {
            String field = indexFields[i];

            types.put(className + "." + field, definition.getTypes()[i]);
            globalFields.add(className + "." + field);

          }

          OLuceneFullTextIndex fullTextIndex = (OLuceneFullTextIndex) index.getInternal();

          globalAnalyzer.add((OLucenePerFieldAnalyzerWrapper) fullTextIndex.queryAnalyzer());

          globalReaders.add(fullTextIndex.searcher().getIndexReader());

        }

      }

      IndexReader indexReader = new MultiReader(globalReaders.toArray(new IndexReader[] {}));

      IndexSearcher searcher = new IndexSearcher(indexReader);

      Map<String, Float> boost = Optional.ofNullable(metadata.<Map<String, Float>>getProperty("boost")).orElse(new HashMap<>());

      OLuceneMultiFieldQueryParser p = new OLuceneMultiFieldQueryParser(types, globalFields.toArray(new String[] {}),
          globalAnalyzer, boost);

      p.setAllowLeadingWildcard(Optional.ofNullable(metadata.<Boolean>getProperty("allowLeadingWildcard")).orElse(false));

      p.setSplitOnWhitespace(Optional.ofNullable(metadata.<Boolean>getProperty("splitOnWhitespace")).orElse(true));

      Object params = keyAndMeta.key.getKeys().get(0);

      Query query = p.parse(params.toString());

      final List<SortField> fields = OLuceneIndexEngineUtils.buildSortFields(metadata);

      OLuceneQueryContext ctx = new OLuceneQueryContext(null, searcher, query, fields);
      return new OLuceneResultSet(this, ctx, metadata);
    } catch (IOException e) {
      OLogManager.instance().error(this, "unable to create multi-reader", e);
    } catch (ParseException e) {
      OLogManager.instance().error(this, "unable to parse query", e);
    }

    return null;
  }

  @Override
  public void put(Object key, Object value) {

  }

  @Override
  public void update(Object key, OIndexKeyUpdater<Object> updater) {
    put(key, updater.update(null, bonsayFileId).getValue());
  }

  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    return false;
  }

  @Override
  public Object getFirstKey() {
    return null;
  }

  @Override
  public Object getLastKey() {
    return null;
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
    return indexName;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return false;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return null;
  }

  @Override
  public String indexName() {
    return null;
  }

  @Override
  public void onRecordAddedToResultSet(OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret,
      final ScoreDoc score) {

    recordId.setContext(new HashMap<String, Object>() {
      {
        HashMap<String, TextFragment[]> frag = queryContext.getFragments();

        frag.entrySet().stream().forEach(f -> {

          TextFragment[] fragments = f.getValue();
          StringBuilder hlField = new StringBuilder();
          for (int j = 0; j < fragments.length; j++) {
            if ((fragments[j] != null) && (fragments[j].getScore() > 0)) {
              hlField.append(fragments[j].toString());
            }
          }
          put("$" + f.getKey() + "_hl", hlField.toString());
        });

        put("$score", score.score);
      }
    });

  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return null;
  }

  @Override
  public Query buildQuery(Object query) {
    return null;
  }

  @Override
  public Analyzer indexAnalyzer() {
    return null;
  }

  @Override
  public Analyzer queryAnalyzer() {
    return null;
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    return false;
  }

  @Override
  public IndexSearcher searcher() {
    return null;
  }

  @Override
  public void release(IndexSearcher searcher) {

  }

  @Override
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
    return null;
  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    return 0;
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    return null;
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    return null;
  }

  @Override
  public boolean isCollectionIndex() {
    return false;
  }

  @Override
  public void freeze(boolean throwException) {

  }

  @Override
  public void release() {

  }

  @Override
  public boolean isFrozen() {
    return false;
  }
}
