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

package com.orientechnologies.lucene.manager;

import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.utils.OLuceneIndexUtils;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by Enrico Risa on 22/04/15.
 */
public class OLuceneFacetManager {

  public static final String          FACET_FIELDS = "facetFields";
  protected TaxonomyWriter            taxonomyWriter;
  protected FacetsConfig              config       = new FacetsConfig();
  protected static final String       FACET        = "_facet";

  protected String                    facetField;
  // protected String facetDim;
  private OLuceneIndexManagerAbstract owner;
  private ODocument                   metadata;

  public OLuceneFacetManager(OLuceneIndexManagerAbstract owner, ODocument metadata) throws IOException {
    this.owner = owner;

    this.metadata = metadata;
    buildFacetIndexIfNeeded();
  }

  protected void buildFacetIndexIfNeeded() throws IOException {

    if (metadata != null && metadata.containsField(FACET_FIELDS)) {
      ODatabaseDocumentInternal database = owner.getDatabase();
      Iterable<String> iterable = metadata.field(FACET_FIELDS);
      if (iterable != null) {
        Directory dir = getTaxDirectory(database);
        taxonomyWriter = new DirectoryTaxonomyWriter(dir, IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        for (String s : iterable) {
          facetField = s;
          // facetField = "facet_" + s;
          // facetDim = s;
          // config.setIndexFieldName(s, "facet_" + s);
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

  protected String getIndexFacetPath(OLocalPaginatedStorage storageLocalAbstract) {
    return storageLocalAbstract.getStoragePath() + File.separator + owner.OLUCENE_BASE_DIR + File.separator + owner.indexName
        + FACET;
  }

  public void delete() {
    ODatabaseDocumentInternal database = owner.getDatabase();
    final OAbstractPaginatedStorage storageLocalAbstract = (OAbstractPaginatedStorage) database.getStorage().getUnderlying();
    if (storageLocalAbstract instanceof OLocalPaginatedStorage) {
      File f = new File(getIndexFacetPath((OLocalPaginatedStorage) storageLocalAbstract));
      OLuceneIndexUtils.deleteFolder(f);
      f = new File(owner.getIndexBasePath((OLocalPaginatedStorage) storageLocalAbstract));
      OLuceneIndexUtils.deleteFolderIfEmpty(f);
    }
  }

  protected Boolean supportsFacets() {
    return taxonomyWriter != null;
  }

  protected boolean isFacetField(String field) {
    if (metadata == null)
      return false;
    if (metadata.field(FACET_FIELDS) == null)
      return false;
    Collection<String> fields = metadata.field(FACET_FIELDS);
    return fields.contains(field);
  }

  protected IndexableField buildFacetField(String f, Object val) {
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
          throw new OIndexEngineException("Cannot facet value " + val + " because it is not a string", null);
        }
      }
      path = values.toArray(new String[values.size()]);
    }
    return new FacetField(f, path);
  }

  public Document buildDocument(Document doc) throws IOException {
    return config.build(taxonomyWriter, doc);
  }

  public void commit() {
    try {
      if (taxonomyWriter != null)
        taxonomyWriter.commit();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void addFacetContext(QueryContext queryContext, Object key) throws IOException {
    queryContext.setFacet(true);
    queryContext.setFacetField(facetField);
    queryContext.setFacetConfig(config);
    // queryContext.setfacetDim(facetDim);
    queryContext.setReader(new DirectoryTaxonomyReader(getTaxDirectory(owner.getDatabase())));

    if (key instanceof OCompositeKey) {
      List<Object> keys = ((OCompositeKey) key).getKeys();
      for (Object o : keys) {
        if (o instanceof Map) {
          String drillDown = (String) ((Map) o).get("drillDown");
          queryContext.setDrillDownQuery(drillDown);
        }
      }
    }
  }
}
