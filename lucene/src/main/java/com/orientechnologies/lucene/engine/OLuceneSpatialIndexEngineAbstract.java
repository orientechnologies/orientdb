/*
 *
 *  * Copyright 2014 Orient Technologies.
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

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.factory.OSpatialStrategyFactory;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.spatial.shape.OShapeBuilder;
import com.orientechnologies.orient.spatial.strategy.SpatialQueryBuilder;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * Created by Enrico Risa on 26/09/15.
 */
public abstract class OLuceneSpatialIndexEngineAbstract extends OLuceneIndexEngineAbstract implements OLuceneSpatialIndexContainer {

  protected final OShapeBuilder     factory;
  protected SpatialContext          ctx;
  protected SpatialStrategy         strategy;

  protected OSpatialStrategyFactory strategyFactory;
  protected SpatialQueryBuilder     queryStrategy;

  public OLuceneSpatialIndexEngineAbstract(String indexName, OShapeBuilder factory) {
    super(indexName);
    this.ctx = factory.context();
    this.factory = factory;
    strategyFactory = new OSpatialStrategyFactory(factory);
    this.queryStrategy = new SpatialQueryBuilder(this, factory);
  }

  @Override
  public void initIndex(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic,
      ODocument metadata) {
    super.initIndex(indexName, indexType, indexDefinition, isAutomatic, metadata);

    strategy = createSpatialStrategy(indexDefinition, metadata);
  }

  protected abstract SpatialStrategy createSpatialStrategy(OIndexDefinition indexDefinition, ODocument metadata);

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    configureAnalyzers(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory) throws IOException {
    configureAnalyzers(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(indexAnalyzer());
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public void init() {

  }

  @Override
  public boolean contains(Object key) {
    return false;
  }

  @Override
  public boolean remove(Object key) {
    return false;
  }

  protected Document newGeoDocument(OIdentifiable oIdentifiable, Shape shape) {

    FieldType ft = new FieldType();
    ft.setIndexOptions(IndexOptions.DOCS);
    ft.setStored(true);

    Document doc = new Document();

    doc.add(OLuceneIndexType.createField(RID, oIdentifiable.getIdentity().toString(), Field.Store.YES,
        Field.Index.NOT_ANALYZED_NO_NORMS));
    for (IndexableField f : strategy.createIndexableFields(shape)) {
      doc.add(f);
    }

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
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
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
  public OIndexKeyCursor keyCursor() {
    return null;
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }

  @Override
  public SpatialStrategy strategy() {
    return strategy;
  }

}
