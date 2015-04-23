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

package com.orientechnologies.lucene.collections;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.*;

/**
 * Created by Enrico Risa on 28/10/14.
 */
public class LuceneResultSet implements Set<OIdentifiable> {

  private TopDocs                     topDocs;
  private Query                       query;
  private OLuceneIndexManagerAbstract manager;
  private QueryContext                queryContext;
  private static Integer              PAGE_SIZE = 50;

  public LuceneResultSet(OLuceneIndexManagerAbstract manager, QueryContext queryContext) {
    this.manager = manager;
    this.queryContext = queryContext;
    this.query = queryContext.query;
    fetchFirstBatch();
    fetchFacet();
  }

  private void fetchFacet() {
    if (queryContext.facet) {
      FacetsCollector facetsCollector = new FacetsCollector(true);

      try {

        String[] pathFacet = null;
        if (queryContext.isDrillDown()) {
          DrillDownQuery drillDownQuery = new DrillDownQuery(queryContext.getFacetConfig(), query);
          String[] path = queryContext.getDrillDownQuery().split(":");
          pathFacet = path[1].split("/");
          drillDownQuery.add(path[0], pathFacet);
          FacetsCollector.search(queryContext.searcher, drillDownQuery, PAGE_SIZE, facetsCollector);
        } else {
          FacetsCollector.search(queryContext.searcher, query, PAGE_SIZE, facetsCollector);
        }

        Facets facets = new FastTaxonomyFacetCounts(queryContext.reader, queryContext.getFacetConfig(), facetsCollector);

        FacetResult facetResult = null;
        if (pathFacet != null) {
          facetResult = facets.getTopChildren(PAGE_SIZE, queryContext.getFacetField(), pathFacet);
        } else {
          facetResult = facets.getTopChildren(PAGE_SIZE, queryContext.getFacetField());
        }

        if (facetResult != null) {
          List<ODocument> documents = new ArrayList<ODocument>();
          // for (FacetResult facetResult : res) {

          ODocument doc = new ODocument();

          doc.field("childCount", facetResult.childCount);
          doc.field("value", facetResult.value);
          doc.field("dim", facetResult.dim);
          List<ODocument> labelsAndValue = new ArrayList<ODocument>();
          for (LabelAndValue labelValue : facetResult.labelValues) {
            ODocument doc1 = new ODocument();
            doc1.field("label", labelValue.label);
            doc1.field("value", labelValue.value);
            labelsAndValue.add(doc1);

          }
          doc.field("labelsValue", labelsAndValue);
          documents.add(doc);
          queryContext.context.setVariable("$facet", documents);
        }
        // }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public int size() {
    return topDocs.totalHits;
  }

  private void fetchFirstBatch() {
    try {

      switch (queryContext.cfg) {

      case NO_FILTER_NO_SORT:
        topDocs = queryContext.searcher.search(query, PAGE_SIZE);
        break;
      case FILTER_SORT:
        topDocs = queryContext.searcher.search(query, queryContext.filter, PAGE_SIZE, queryContext.sort);
        break;
      case FILTER:
        topDocs = queryContext.searcher.search(query, queryContext.filter, PAGE_SIZE);
        break;
      case SORT:
        topDocs = queryContext.searcher.search(query, PAGE_SIZE, queryContext.sort);
        break;
      }
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
    }
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLuceneResultSetIterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  private class OLuceneResultSetIterator implements Iterator<OIdentifiable> {

    ScoreDoc[]  array;
    private int index;
    private int localIndex;
    private int totalHits;

    public OLuceneResultSetIterator() {
      totalHits = topDocs.totalHits;
      index = 0;
      localIndex = 0;
      array = topDocs.scoreDocs;
      manager.sendTotalHits(queryContext.context, topDocs);
    }

    @Override
    public boolean hasNext() {
      return index < totalHits;
    }

    @Override
    public OIdentifiable next() {
      if (localIndex == array.length) {
        localIndex = 0;
        fetchMoreResult();
      }
      final ScoreDoc score = array[localIndex++];
      Document ret = null;
      OContextualRecordId res = null;
      try {
        ret = queryContext.searcher.doc(score.doc);
        String rId = ret.get(OLuceneIndexManagerAbstract.RID);
        res = new OContextualRecordId(rId);
        manager.onRecordAddedToResultSet(queryContext, res, ret, score);
      } catch (IOException e) {
        e.printStackTrace();
      }
      index++;
      return res;
    }

    private void fetchMoreResult() {

      TopDocs topDocs = null;
      try {

        switch (queryContext.cfg) {

        case NO_FILTER_NO_SORT:
          topDocs = queryContext.searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE);
          break;
        case FILTER_SORT:
          topDocs = queryContext.searcher.searchAfter(array[array.length - 1], query, queryContext.filter, PAGE_SIZE,
              queryContext.sort);
          break;
        case FILTER:
          topDocs = queryContext.searcher.searchAfter(array[array.length - 1], query, queryContext.filter, PAGE_SIZE);
          break;
        case SORT:
          topDocs = queryContext.searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE, queryContext.sort);
          break;
        }
        array = topDocs.scoreDocs;
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
      }

    }

    @Override
    public void remove() {

    }
  }
}
