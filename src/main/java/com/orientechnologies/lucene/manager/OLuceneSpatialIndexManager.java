package com.orientechnologies.lucene.manager;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.spatial4j.core.context.SpatialContext;
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
    return null;
  }

  @Override
  public void put(Object key, Object value) {

    OCompositeKey compositeKey = (OCompositeKey) key;
    List<Object> numbers = compositeKey.getKeys();
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Number x = (Number) numbers.get(0);
      Number y = (Number) numbers.get(1);
      addDocument(newGeoDocument(oIdentifiable.getIdentity().toString(), ctx.makePoint(x.doubleValue(), y.doubleValue())));
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
  public void getEntriesMajor(Object fromKey, boolean isInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesMinor(Object toKey, boolean isInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public void getEntriesBetween(Object iRangeFrom, Object iRangeTo, boolean iInclusive, ValuesTransformer transformer,
      EntriesResultListener entriesResultListener) {

  }

  @Override
  public boolean hasRangeQuerySupport() {
    return false;
  }
}
