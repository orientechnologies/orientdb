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

package com.orientechnologies.lucene.collections;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Enrico Risa on 28/10/14.
 */
public class OLuceneResultSet extends OLuceneAbstractResultSet {

  public OLuceneResultSet(OLuceneIndexEngine engine, OLuceneQueryContext queryContext) {
    super(engine, queryContext);
  }

  @Override
  public int size() {
    return topDocs.totalHits;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLuceneResultSetIterator();
  }

  private class OLuceneResultSetIterator implements Iterator<OIdentifiable> {

    private ScoreDoc[] array;
    private int        index;
    private int        localIndex;
    private int        totalHits;

    public OLuceneResultSetIterator() {
      totalHits = topDocs.totalHits;
      index = 0;
      localIndex = 0;
      array = topDocs.scoreDocs;
      OLuceneIndexEngineAbstract.sendTotalHits(indexName, queryContext.context, topDocs.totalHits);
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
        ret = queryContext.getSearcher().doc(score.doc);
        String rId = ret.get(OLuceneIndexEngineAbstract.RID);
        res = new OContextualRecordId(rId);
        engine.onRecordAddedToResultSet(queryContext, res, ret, score);
      } catch (IOException e) {
        //TODO handle in a proper way
        e.printStackTrace();
      }
      index++;

      return res;
    }

    private void fetchMoreResult() {

      TopDocs topDocs = null;
      try {

        IndexSearcher searcher = queryContext.getSearcher();
        switch (queryContext.cfg) {

        case NO_FILTER_NO_SORT:
          topDocs = searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE);
          break;
        case FILTER_SORT:
          topDocs = searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE, queryContext.sort);
          break;
        case FILTER:
          topDocs = searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE);
          break;
        case SORT:
          topDocs = searcher.searchAfter(array[array.length - 1], query, PAGE_SIZE, queryContext.sort);
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
