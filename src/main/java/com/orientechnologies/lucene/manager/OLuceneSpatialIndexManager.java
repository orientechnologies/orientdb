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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.lucene.shape.OShapeFactory;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class OLuceneSpatialIndexManager extends OLuceneIndexManagerAbstract {

  private final OShapeFactory factory;
  private SpatialContext      ctx;
  private SpatialStrategy     strategy;

  public OLuceneSpatialIndexManager(OShapeFactory factory) {
    super();
    this.ctx = SpatialContext.GEO;
    this.factory = factory;
    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, 11);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "location");
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public void init() {

  }

  @Override
  public void deleteWithoutLoad(String indexName) {

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
  public ORID getIdentity() {
    return null;
  }

  @Override
  public Object get(Object key) {
    try {
      if (key instanceof OSpatialCompositeKey) {
        final OSpatialCompositeKey newKey = (OSpatialCompositeKey) key;

        final SpatialOperation strategy = newKey.getOperation() != null ? newKey.getOperation() : SpatialOperation.Intersects;

        if (SpatialOperation.Intersects.equals(strategy))
          return searchIntersect(newKey, newKey.getMaxDistance(), newKey.getContext());
        else if (SpatialOperation.IsWithin.equals(strategy))
          return searchWithin(newKey, newKey.getContext());

      } else if (key instanceof OCompositeKey)
        return searchIntersect((OCompositeKey) key, 0, null);

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on getting entry against Lucene index", e);
    }

    return null;
  }

  public Object searchIntersect(OCompositeKey key, double distance, OCommandContext context) throws IOException {

    double lat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    double lng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    SpatialOperation operation = SpatialOperation.Intersects;
    Point p = ctx.makePoint(lng, lat);
    SpatialArgs args = new SpatialArgs(operation, ctx.makeCircle(lng, lat,
        DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM)));
    Filter filter = strategy.makeFilter(args);

    IndexSearcher searcher = getSearcher();
    ValueSource valueSource = strategy.makeDistanceValueSource(p);
    Sort distSort = new Sort(valueSource.getSortField(false)).rewrite(searcher);

    Integer limit = null;
    if (context != null) {
      limit = (Integer) context.getVariable("$limit");
    }
    int limitDoc = (limit != null && limit > 0) ? limit : Integer.MAX_VALUE;
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filter, limitDoc, distSort);
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    for (ScoreDoc s : scoreDocs) {
      Document doc = searcher.doc(s.doc);
      Point docPoint = (Point) ctx.readShape(doc.get(strategy.getFieldName()));
      double docDistDEG = ctx.getDistCalc().distance(args.getShape().getCenter(), docPoint);
      final double docDistInKM = DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
      result.add(new OContextualRecordId(doc.get(RID)).setContext(new HashMap<String, Object>() {
        {
          put("distance", docDistInKM);
        }
      }));

    }
    return result;
  }

  public Object searchWithin(OSpatialCompositeKey key, OCommandContext context) throws IOException {

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    Shape shape = factory.makeShape(key, ctx);
    if (shape == null)
      return null;
    SpatialArgs args = new SpatialArgs(SpatialOperation.IsWithin, shape);
    IndexSearcher searcher = getSearcher();

    Integer limit = null;
    if (context != null) {
      limit = (Integer) context.getVariable("$limit");
    }
    int limitDoc = (limit != null && limit > 0) ? limit : Integer.MAX_VALUE;
    Filter filter = strategy.makeFilter(args);
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filter, limitDoc);

    sendTotalHits(context, topDocs);
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
    for (ScoreDoc s : scoreDocs) {
      Document doc = searcher.doc(s.doc);
      result.add(new ORecordId(doc.get(RID)));

    }
    return result;
  }

  @Override
  public void put(Object key, Object value) {

    OCompositeKey compositeKey = (OCompositeKey) key;
    if (key instanceof OCompositeKey) {
    }
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      addDocument(newGeoDocument(oIdentifiable.getIdentity().toString(), factory.makeShape(compositeKey, ctx)));
    }
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

  private Document newGeoDocument(String rid, Shape shape) {

    FieldType ft = new FieldType();
    ft.setIndexed(true);
    ft.setStored(true);

    Document doc = new Document();

    doc.add(new TextField(RID, rid, Field.Store.YES));
    for (IndexableField f : strategy.createIndexableFields(shape)) {
      doc.add(f);
    }

    doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));

    return doc;
  }
}
