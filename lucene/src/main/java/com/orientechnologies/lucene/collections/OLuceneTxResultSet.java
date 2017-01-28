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
import com.orientechnologies.lucene.tx.OLuceneTxChangesAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Enrico Risa on 16/09/15.
 */
public class OLuceneTxResultSet extends OLuceneAbstractResultSet {

  private final String indexName;
  protected int deletedMatchCount = 0;

  public OLuceneTxResultSet(OLuceneIndexEngine engine, OLuceneQueryContext queryContext) {
    super(engine, queryContext);

    deletedMatchCount = calculateDeletedMatch();
    indexName = engine.indexName();
  }

  private int calculateDeletedMatch() {
    return (int) queryContext.changes().deletedDocs(query);
  }

  @Override
  public int size() {
    return Math.max(0, topDocs.totalHits - deletedMatchCount);
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLuceneResultSetIteratorTx();
  }

  private class OLuceneResultSetIteratorTx implements Iterator<OIdentifiable> {

    private ScoreDoc[] array;
    private int        index;
    private int        localIndex;
    private int        totalHits;

    public OLuceneResultSetIteratorTx() {
      totalHits = topDocs.totalHits;
      index = 0;
      localIndex = 0;
      array = topDocs.scoreDocs;
      OLuceneIndexEngineAbstract.sendTotalHits(indexName, queryContext.context, topDocs.totalHits - deletedMatchCount);
    }

    @Override
    public boolean hasNext() {
      return index < (totalHits - deletedMatchCount);
    }

    @Override
    public OIdentifiable next() {
      ScoreDoc scoreDoc;
      OContextualRecordId res;
      Document doc;
      do {
        scoreDoc = fetchNext();
        doc = toDocument(scoreDoc);
        res = toRecordId(doc, scoreDoc);
      } while (isToSkip(res, doc));
      index++;
      return res;
    }

    protected ScoreDoc fetchNext() {
      if (localIndex == array.length) {
        localIndex = 0;
        fetchMoreResult();
      }
      final ScoreDoc score = array[localIndex++];
      return score;
    }

    private Document toDocument(ScoreDoc score) {
      Document ret = null;

      try {
        ret = queryContext.getSearcher().doc(score.doc);

      } catch (IOException e) {
        e.printStackTrace();
      }
      return ret;
    }

    private OContextualRecordId toRecordId(Document doc, ScoreDoc score) {
      String rId = doc.get(OLuceneIndexEngineAbstract.RID);
      OContextualRecordId res = new OContextualRecordId(rId);
      engine.onRecordAddedToResultSet(queryContext, res, doc, score);
      return res;
    }

    private boolean isToSkip(OContextualRecordId res, Document doc) {
      return isDeleted(res, doc) || isUpdatedDiskMatch(res, doc);
    }

    private void fetchMoreResult() {

      TopDocs topDocs = null;
      try {

        switch (queryContext.cfg) {

        case NO_FILTER_NO_SORT:
          topDocs = queryContext.getSearcher().searchAfter(array[array.length - 1], query, PAGE_SIZE);
          break;
        case FILTER_SORT:
          topDocs = queryContext.getSearcher().searchAfter(array[array.length - 1], query, PAGE_SIZE, queryContext.sort);
          break;
        case FILTER:
          topDocs = queryContext.getSearcher().searchAfter(array[array.length - 1], query, PAGE_SIZE);
          break;
        case SORT:
          topDocs = queryContext.getSearcher().searchAfter(array[array.length - 1], query, PAGE_SIZE, queryContext.sort);
          break;
        }
        array = topDocs.scoreDocs;
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
      }

    }

    private boolean isDeleted(OIdentifiable value, Document doc) {
      return queryContext.changes().isDeleted(doc, null, value);
    }

    private boolean isUpdatedDiskMatch(OIdentifiable value, Document doc) {
      return isUpdated(value) && !isTempMatch(doc);
    }

    private boolean isUpdated(OIdentifiable value) {
      return queryContext.changes().isUpdated(null, null, value);
    }

    private boolean isTempMatch(Document doc) {
      return doc.get(OLuceneTxChangesAbstract.TMP) != null;
    }

    @Override
    public void remove() {

    }

  }
}
