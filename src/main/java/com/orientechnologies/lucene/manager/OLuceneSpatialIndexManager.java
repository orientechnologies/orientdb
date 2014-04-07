package com.orientechnologies.lucene.manager;

import java.io.IOException;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.*;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.orientechnologies.lucene.collections.OSpatialCompositeKey;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneSpatialIndexManager extends OLuceneIndexManagerAbstract {

  private SpatialContext  ctx;
  private SpatialStrategy strategy;

  public OLuceneSpatialIndexManager() {
    super();
    this.ctx = SpatialContext.GEO;

    SpatialPrefixTree grid = new GeohashPrefixTree(ctx, 11);
    this.strategy = new RecursivePrefixTreeStrategy(grid, "location");
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory) throws IOException {
    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_47, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    return new IndexWriter(directory, iwc);
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
  public Iterator<Map.Entry> inverseIterator() {
    return null;
  }

  @Override
  public Iterator valuesIterator() {
    return null;
  }

  @Override
  public Iterator inverseValuesIterator() {
    return null;
  }

  @Override
  public Iterable<Object> keys() {
    return null;
  }

  @Override
  public Object get(Object key) {
    if (key instanceof OSpatialCompositeKey) {

      OSpatialCompositeKey newKey = (OSpatialCompositeKey) key;

      SpatialOperation strategy = newKey.getOperation() != null ? newKey.getOperation() : SpatialOperation.Intersects;

      if (SpatialOperation.Intersects.equals(strategy)) {
        try {
          return searchIntersect(newKey, newKey.getMaxDistance());
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else if (SpatialOperation.IsWithin.equals(strategy)) {
        try {
          return searchBBox(newKey);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    } else if (key instanceof OCompositeKey) {
      try {
        return searchIntersect((OCompositeKey) key, 0);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return null;
  }

  public Object searchIntersect(OCompositeKey key, double distance) throws IOException {

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

    int limit = 1000;
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filter, limit, distSort);
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;

    for (ScoreDoc s : scoreDocs) {

      Document doc = searcher.doc(s.doc);
      result.add(new ORecordId(doc.get(RID)));

    }
    return result;
  }

  public Object searchBBox(OCompositeKey key) throws IOException {

    Double minLat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(0), Double.class)).doubleValue();
    Double minLng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(1), Double.class)).doubleValue();
    Double maxLat = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(2), Double.class)).doubleValue();
    Double maxLng = ((Double) OType.convert(((OCompositeKey) key).getKeys().get(3), Double.class)).doubleValue();
    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    SpatialArgs args = new SpatialArgs(SpatialOperation.IsWithin, ctx.makeRectangle(minLat, maxLat, minLng, maxLng));
    IndexSearcher searcher = getSearcher();

    Filter filter = strategy.makeFilter(args);
    int limit = 1000;
    TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), filter, limit);

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
    List<Object> numbers = compositeKey.getKeys();
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Number x = (Number) numbers.get(0);
      Number y = (Number) numbers.get(1);
      addDocument(newGeoDocument(oIdentifiable.getIdentity().toString(), ctx.makePoint(y.doubleValue(), x.doubleValue())));
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
  public void getValuesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive, boolean ascSortOrder,
      ValuesTransformer transformer, ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getValuesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getValuesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer,
      ValuesResultListener valuesResultListener) {

  }

  @Override
  public void getEntriesMajor(Object fromKey, boolean isInclusive, boolean ascOrder, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, boolean ascOrder, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, boolean ascOrder,
      ValuesTransformer transformer, EntriesResultListener entriesResultListener) {

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }
}
