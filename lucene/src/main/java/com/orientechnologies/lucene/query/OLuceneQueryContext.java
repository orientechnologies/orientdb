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

package com.orientechnologies.lucene.query;

import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.command.OCommandContext;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

import java.io.IOException;

/**
 * Created by Enrico Risa on 08/01/15.
 */
public class OLuceneQueryContext {

  private final OCommandContext  context;
  private final IndexSearcher    searcher;
  private final Query            query;
  private final Sort             sort;
  private       OLuceneTxChanges changes;
  private       QueryContextCFG  cfg;
  private       TaxonomyReader   reader;
  private       FacetsConfig     facetConfig;
  private       String           facetField;
  private       String           drillDownQuery;
  private       boolean          facet;
  private       boolean          drillDown;

  public OLuceneQueryContext(OCommandContext context, IndexSearcher searcher, Query query) {
    this(context, searcher, query, null);
  }

  public OLuceneQueryContext(OCommandContext context, IndexSearcher searcher, Query query, Sort sort) {
    this.context = context;
    this.searcher = searcher;
    this.query = query;
    this.sort = sort;
    if (sort != null)
      cfg = QueryContextCFG.SORT;
    else
      cfg = QueryContextCFG.FILTER;

    facet = false;
    drillDown = false;
  }

  public OLuceneQueryContext setFacet(boolean facet) {
    this.facet = facet;
    return this;
  }

  public OLuceneQueryContext setReader(TaxonomyReader reader) {
    this.reader = reader;
    return this;
  }

  public FacetsConfig getFacetConfig() {
    return facetConfig;
  }

  public void setFacetConfig(FacetsConfig facetConfig) {
    this.facetConfig = facetConfig;
  }

  public String getFacetField() {
    return facetField;
  }

  public void setFacetField(String facetField) {
    this.facetField = facetField;
  }

  public boolean isDrillDown() {
    return drillDown;
  }

  public String getDrillDownQuery() {
    return drillDownQuery;
  }

  public void setDrillDownQuery(String drillDownQuery) {
    this.drillDownQuery = drillDownQuery;
    drillDown = drillDownQuery != null;
  }

  public boolean isInTx() {
    return changes != null;
  }

  public OLuceneQueryContext withChanges(OLuceneTxChanges changes) {
    this.changes = changes;
    return this;
  }

  public OLuceneTxChanges changes() {
    return changes;
  }

  public OCommandContext getContext() {
    return context;
  }

  public Query getQuery() {
    return query;
  }

  public Sort getSort() {
    return sort;
  }

  public IndexSearcher getSearcher() throws IOException {

    return changes == null ?
        searcher :
        new IndexSearcher(new MultiReader(searcher.getIndexReader(), changes.searcher().getIndexReader()));
  }

  public QueryContextCFG getCfg() {
    return cfg;
  }

  public enum QueryContextCFG {
    FILTER, SORT
  }

}
