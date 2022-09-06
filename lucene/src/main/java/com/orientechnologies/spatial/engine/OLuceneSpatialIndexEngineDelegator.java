/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 * <p>
 * For more information: http://www.orientdb.com
 */
package com.orientechnologies.spatial.engine;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexKeyUpdater;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.spatial.SpatialStrategy;

/** Created by Enrico Risa on 04/09/15. */
public class OLuceneSpatialIndexEngineDelegator
    implements OLuceneIndexEngine, OLuceneSpatialIndexContainer {

  private final OStorage storage;
  private final String indexName;
  private OLuceneSpatialIndexEngineAbstract delegate;
  private final int id;

  public OLuceneSpatialIndexEngineDelegator(int id, String name, OStorage storage, int version) {
    this.id = id;

    this.indexName = name;
    this.storage = storage;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {
    if (delegate == null) {
      if (OClass.INDEX_TYPE.SPATIAL.name().equalsIgnoreCase(indexType)) {
        if (indexDefinition.getFields().size() > 1) {
          delegate =
              new OLuceneLegacySpatialIndexEngine(storage, indexName, id, OShapeFactory.INSTANCE);
        } else {
          delegate =
              new OLuceneGeoSpatialIndexEngine(storage, indexName, id, OShapeFactory.INSTANCE);
        }

        delegate.init(indexName, indexType, indexDefinition, isAutomatic, metadata);
      } else {
        throw new IllegalStateException("Invalid index type " + indexType);
      }
    }
  }

  @Override
  public void flush() {
    delegate.flush();
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
    if (delegate != null) delegate.delete(atomicOperation);
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
      OEncryption encryption) {
    if (delegate != null)
      delegate.load(
          indexName,
          valueSerializer,
          isAutomatic,
          keySerializer,
          keyTypes,
          nullPointerSupport,
          keySize,
          engineProperties,
          encryption);
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) {
    return delegate.remove(atomicOperation, key);
  }

  @Override
  public void clear(OAtomicOperation atomicOperation) {

    delegate.clear(atomicOperation);
  }

  @Override
  public void close() {

    delegate.close();
  }

  @Override
  public Object get(Object key) {
    return delegate.get(key);
  }

  @Override
  public void put(OAtomicOperation atomicOperation, Object key, Object value) {

    try {
      delegate.put(atomicOperation, key, value);
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during insertion of key " + key + " in index " + indexName),
          e);
    }
  }

  @Override
  public void update(
      OAtomicOperation atomicOperation, Object key, OIndexKeyUpdater<Object> updater) {
    try {
      delegate.update(atomicOperation, key, updater);
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during update of key " + key + " in index " + indexName), e);
    }
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation, Object key, ORID value, Validator<Object, ORID> validator) {
    try {
      return delegate.validatedPut(atomicOperation, key, value, validator);
    } catch (IOException e) {
      throw OException.wrapException(
          new OIndexException("Error during insertion of key " + key + " in index " + indexName),
          e);
    }
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    return delegate.iterateEntriesBetween(
        rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder, transformer);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return delegate.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder, transformer);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return delegate.iterateEntriesMinor(toKey, isInclusive, ascSortOrder, transformer);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    return delegate.stream(valuesTransformer);
  }

  @Override
  public Stream<ORawPair<Object, ORID>> descStream(ValuesTransformer valuesTransformer) {
    return delegate.descStream(valuesTransformer);
  }

  @Override
  public Stream<Object> keyStream() {
    return delegate.keyStream();
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
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String indexName() {
    return indexName;
  }

  @Override
  public void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext,
      OContextualRecordId recordId,
      Document ret,
      ScoreDoc score) {
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
  public boolean remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public boolean remove(Object key, OIdentifiable value) {
    return delegate.remove(key, value);
  }

  @Override
  public IndexSearcher searcher() {
    return delegate.searcher();
  }

  @Override
  public void release(IndexSearcher searcher) {
    delegate.release(searcher);
  }

  @Override
  public SpatialStrategy strategy() {
    return delegate.strategy();
  }

  @Override
  public boolean isLegacy() {
    return delegate.isLegacy();
  }

  @Override
  public Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes) {
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
  public boolean isCollectionIndex() {
    return delegate.isCollectionIndex();
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
    return true; // do nothing
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return delegate.getIndexNameByKey(key);
  }

  @Override
  public void updateUniqueIndexVersion(final Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(final Object key) {
    return 0; // not implemented
  }

  public OLuceneIndexEngine getDelegate() {
    return delegate;
  }
}
