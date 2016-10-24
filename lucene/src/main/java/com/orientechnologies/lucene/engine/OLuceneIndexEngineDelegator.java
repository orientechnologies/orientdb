/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.engine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.lucene.builder.ODocBuilder;
import com.orientechnologies.lucene.builder.OQueryBuilderImpl;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE.FULLTEXT;

/**
 * Created by Enrico Risa on 04/09/15.
 */
public class OLuceneIndexEngineDelegator implements OLuceneIndexEngine, OFreezableStorageComponent {

  private final Boolean            durableInNonTxMode;
  private final OStorage           storage;
  private final int                version;
  private final String             indexName;
  private       OLuceneIndexEngine delegate;

  public OLuceneIndexEngineDelegator(String name, Boolean durableInNonTxMode, OStorage storage, int version) {

    this.indexName = name;
    this.durableInNonTxMode = durableInNonTxMode;
    this.storage = storage;
    this.version = version;
  }

  @Override
  public void flush() {
    delegate.flush();
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata) {
    //NOOP
  }

  @Override
  public void delete() {
    delegate.delete();
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    if (delegate != null)
      delegate.deleteWithoutLoad(indexName);
  }

  @Override
  public void load(final String indexName, final OBinarySerializer valueSerializer, final boolean isAutomatic,
      final OBinarySerializer keySerializer, final OType[] keyTypes, final boolean nullPointerSupport, final int keySize,
      final Map<String, String> engineProperties) {
    if (delegate != null)
      delegate
          .load(indexName, valueSerializer, isAutomatic, keySerializer, keyTypes, nullPointerSupport, keySize, engineProperties);
  }

  @Override
  public boolean contains(Object key) {
    return delegate.contains(key);
  }

  @Override
  public boolean remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public void clear() {

    delegate.clear();
  }

  @Override
  public void close() {
    if (delegate != null) {
      delegate.close();
    }
  }

  @Override
  public Object get(Object key) {
    return delegate.get(key);
  }

  @Override
  public void put(Object key, Object value) {

    delegate.put(key, value);
  }

  @Override
  public boolean validatedPut(Object key, OIdentifiable value, Validator<Object, OIdentifiable> validator) {
    return delegate.validatedPut(key, value, validator);
  }

  @Override
  public Object getFirstKey() {
    return delegate.getFirstKey();
  }

  @Override
  public Object getLastKey() {
    return delegate.getFirstKey();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return delegate.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return delegate.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return delegate.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    return delegate.cursor(valuesTransformer);
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    return delegate.descCursor(valuesTransformer);
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return delegate.keyCursor();
  }

  @Override
  public long size(ValuesTransformer transformer) {
    return delegate.size(transformer);
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return delegate.hasRangeQuerySupport();
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getIndexNameByKey(final Object key) {
    return delegate.getIndexNameByKey(key);
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
    if (delegate == null) {
      if (FULLTEXT.name().equalsIgnoreCase(indexType)) {

        delegate = new OLuceneFullTextIndexEngine(storage, indexName, new ODocBuilder(), new OQueryBuilderImpl(metadata));
      }

      delegate.init(indexName, indexType, indexDefinition, isAutomatic, metadata);
    }
  }

  @Override
  public String indexName() {
    return indexName;
  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret, ScoreDoc score) {
    delegate.onRecordAddedToResultSet(queryContext, recordId, ret, score);
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    return delegate.buildDocument(key, value);
  }

  @Override
  public Query buildQuery(Object query) {
    return delegate.buildQuery(query);
  }

  @Override
  public Analyzer indexAnalyzer() {
    return delegate.indexAnalyzer();
  }

  @Override
  public Analyzer queryAnalyzer() {
    return delegate.queryAnalyzer();
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    return delegate.remove(key, value);
  }

  @Override
  public IndexSearcher searcher() throws IOException {
    return delegate.searcher();
  }

  @Override
  public Object getInTx(Object key, OLuceneTxChanges changes) {
    return delegate.getInTx(key, changes);
  }

  @Override
  public long sizeInTx(OLuceneTxChanges changes) {
    return delegate.sizeInTx(changes);
  }

  @Override
  public OLuceneTxChanges buildTxChanges() throws IOException {
    return delegate.buildTxChanges();
  }

  @Override
  public Query deleteQuery(Object key, OIdentifiable value) {
    return delegate.deleteQuery(key, value);
  }

  @Override
  public void freeze(boolean throwException) {

    delegate.freeze(throwException);
  }

  @Override
  public void release() {
    delegate.release();
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    return delegate.acquireAtomicExclusiveLock(key);
  }
}
