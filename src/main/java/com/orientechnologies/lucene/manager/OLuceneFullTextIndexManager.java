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

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.collections.LuceneResultSet;
import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.utils.OLuceneIndexUtils;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class OLuceneFullTextIndexManager extends OLuceneIndexManagerAbstract {

  public static final String    FACET_FIELDS = "facetFields";
  protected TaxonomyWriter      taxonomyWriter;
  protected FacetsConfig        config       = new FacetsConfig();
  protected static final String FACET        = "_facet";

  protected String              facetField;

  public OLuceneFullTextIndexManager() {
  }

  @Override
  public IndexWriter createIndexWriter(Directory directory, ODocument metadata) throws IOException {

    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getLuceneVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

    buildFacetIndexIfNeeded(metadata);

    return new IndexWriter(directory, iwc);
  }

  protected void buildFacetIndexIfNeeded(ODocument metadata) throws IOException {

    if (metadata != null && metadata.containsField(FACET_FIELDS)) {
      ODatabaseDocumentInternal database = getDatabase();

      Iterable<String> iterable = metadata.field(FACET_FIELDS);
      if (iterable != null) {
        Directory dir = getTaxDirectory(database);
        taxonomyWriter = new DirectoryTaxonomyWriter(dir, IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        for (String s : iterable) {
          facetField = "facet_" + s;
          config.setIndexFieldName(s, "facet_" + s);
          config.setHierarchical(s, true);
        }
      }

    }
  }

  private Directory getTaxDirectory(ODatabaseDocumentInternal database) throws IOException {
    Directory dir = null;
    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) database.getStorage().getUnderlying();
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      String pathname = getIndexFacetPath((OLocalPaginatedStorage) storageLocalAbstract);
      dir = NIOFSDirectory.open(new File(pathname));
    } else {
      dir = new RAMDirectory();
    }
    return dir;
  }

  @Override
  public IndexWriter openIndexWriter(Directory directory, ODocument metadata) throws IOException {
    Analyzer analyzer = getAnalyzer(metadata);
    Version version = getLuceneVersion(metadata);
    IndexWriterConfig iwc = new IndexWriterConfig(version, analyzer);
    iwc.setOpenMode(IndexWriterConfig.OpenMode.APPEND);
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
    Query q = null;

    try {
      q = OLuceneIndexType.createFullQuery(index, key, mgrWriter.getIndexWriter().getAnalyzer(), getLuceneVersion(metadata));
      OCommandContext context = null;
      if (key instanceof OFullTextCompositeKey) {
        context = ((OFullTextCompositeKey) key).getContext();
      }
      if (supportsFacets()) {

      }
      return getResults(q, context);
    } catch (ParseException e) {
      throw new OIndexEngineException("Error parsing lucene query ", e);
    }
  }

  @Override
  public void put(Object key, Object value) {
    Set<OIdentifiable> container = (Set<OIdentifiable>) value;
    for (OIdentifiable oIdentifiable : container) {
      Document doc = new Document();
      doc.add(OLuceneIndexType.createField(RID, oIdentifiable, oIdentifiable.getIdentity().toString(), Field.Store.YES,
          Field.Index.NOT_ANALYZED_NO_NORMS));
      int i = 0;
      for (String f : index.getFields()) {

        Object val = null;
        if (key instanceof OCompositeKey) {
          val = ((OCompositeKey) key).getKeys().get(i);
          i++;
        } else {
          val = key;
        }
        if (val != null) {

          if (supportsFacets() && isFacetField(f)) {
            doc.add(buildFacetField(f, val));
          } else {
            doc.add(OLuceneIndexType.createField(f, oIdentifiable, val, Field.Store.NO, Field.Index.ANALYZED));
          }
        }

      }
      if (supportsFacets()) {
        try {
          addDocument(config.build(taxonomyWriter, doc));
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        addDocument(doc);
      }

      try {
        if (taxonomyWriter != null)
          taxonomyWriter.commit();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  private IndexableField buildFacetField(String f, Object val) {
    String[] path = null;
    if (val instanceof String) {

      path = ((String) val).split("/");
      // path = new String[1];
      // path[0] = (String) val;
    } else if (val instanceof Iterable) {
      Iterable iterable = (Iterable) val;
      List<String> values = new ArrayList<String>();
      for (Object s : iterable) {
        if (s instanceof String) {
          values.add((String) s);
        } else {
          throw new OIndexEngineException("Cannot facet value " + val + " because it is not a string");
        }
      }
      path = values.toArray(new String[values.size()]);
    }
    return new FacetField(f, path);
  }

  protected boolean isFacetField(String field) {

    if (metadata == null)
      return false;
    if (metadata.field(FACET_FIELDS) == null)
      return false;
    Collection<String> fields = metadata.field(FACET_FIELDS);
    return fields.contains(field);
  }

  protected Boolean supportsFacets() {
    return taxonomyWriter != null;
  }

  private Set<OIdentifiable> getResults(Query query, OCommandContext context) {

    try {
      IndexSearcher searcher = getSearcher();
      QueryContext queryContext = new QueryContext(context, searcher, query);
      if (supportsFacets()) {
        queryContext.setFacet(true);
        queryContext.setFacefField(facetField);
        queryContext.setFacetConfig(config);
        queryContext.setReader(new DirectoryTaxonomyReader(getTaxDirectory(getDatabase())));
      }
      return new LuceneResultSet(this, queryContext);
    } catch (IOException e) {
      throw new OIndexException("Error reading from Lucene index", e);
    }

  }

  @Override
  public void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret, final ScoreDoc score) {
    recordId.setContext(new HashMap<String, Object>() {
      {
        put("score", score.score);
      }
    });
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
    return new OIndexKeyCursor() {
      @Override
      public Object next(int prefetchSize) {
        return null;
      }
    };
  }

  protected String getIndexFacetPath(OLocalPaginatedStorage storageLocalAbstract) {
    return storageLocalAbstract.getStoragePath() + File.separator + OLUCENE_BASE_DIR + File.separator + indexName + FACET;
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
  public void delete() {
    super.delete();

    ODatabaseDocumentInternal database = getDatabase();
    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) database.getStorage().getUnderlying();
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      File f = new File(getIndexFacetPath((OLocalPaginatedStorage) storageLocalAbstract));
      OLuceneIndexUtils.deleteFolder(f);
      f = new File(getIndexBasePath((OLocalPaginatedStorage) storageLocalAbstract));
      OLuceneIndexUtils.deleteFolderIfEmpty(f);
    }
  }
}
