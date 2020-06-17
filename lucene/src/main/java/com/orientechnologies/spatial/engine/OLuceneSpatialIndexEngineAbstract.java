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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.lucene.builder.OLuceneIndexType;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.engine.OLuceneIndexWriterFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.spatial.factory.OSpatialStrategyFactory;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilder;
import java.io.IOException;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.store.Directory;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

/** Created by Enrico Risa on 26/09/15. */
public abstract class OLuceneSpatialIndexEngineAbstract extends OLuceneIndexEngineAbstract
    implements OLuceneSpatialIndexContainer {

  protected final OShapeBuilder factory;
  protected SpatialContext ctx;
  protected SpatialStrategy strategy;

  protected OSpatialStrategyFactory strategyFactory;
  protected SpatialQueryBuilder queryStrategy;

  public OLuceneSpatialIndexEngineAbstract(
      OStorage storage, String indexName, int id, OShapeBuilder factory) {
    super(id, storage, indexName);
    this.ctx = factory.context();
    this.factory = factory;
    strategyFactory = new OSpatialStrategyFactory(factory);
    this.queryStrategy = new SpatialQueryBuilder(this, factory);
  }

  @Override
  public void init(
      String indexName,
      String indexType,
      OIndexDefinition indexDefinition,
      boolean isAutomatic,
      ODocument metadata) {
    super.init(indexName, indexType, indexDefinition, isAutomatic, metadata);

    strategy = createSpatialStrategy(indexDefinition, metadata);
  }

  protected abstract SpatialStrategy createSpatialStrategy(
      OIndexDefinition indexDefinition, ODocument metadata);

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {
    OLuceneIndexWriterFactory fc = new OLuceneIndexWriterFactory();

    OLogManager.instance().debug(this, "Creating Lucene index in '%s'...", directory);

    return fc.createIndexWriter(directory, metadata, indexAnalyzer());
  }

  @Override
  public boolean remove(OAtomicOperation atomicOperation, Object key) {
    return false;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesBetween(
      Object rangeFrom,
      boolean fromInclusive,
      Object rangeTo,
      boolean toInclusive,
      boolean ascSortOrder,
      ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMajor(
      Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> iterateEntriesMinor(
      Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return null;
  }

  @Override
  public Stream<ORawPair<Object, ORID>> stream(ValuesTransformer valuesTransformer) {
    return null;
  }

  @Override
  public Stream<Object> keyStream() {
    return null;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  protected Document newGeoDocument(OIdentifiable oIdentifiable, Shape shape) {

    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.setStored(true);

    Document doc = new Document();

    doc.add(
        OLuceneIndexType.createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES));

    for (IndexableField f : strategy.createIndexableFields(shape)) {
      doc.add(f);
    }

    //noinspection deprecation
    doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));
    return doc;
  }

  @Override
  public Document buildDocument(Object key, OIdentifiable value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Query buildQuery(Object query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SpatialStrategy strategy() {
    return strategy;
  }
}
