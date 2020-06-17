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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineUtils;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChangesAbstract;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryTermScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.highlight.TokenSources;

/** Created by Enrico Risa on 16/09/15. */
public class OLuceneResultSet implements Set<OIdentifiable> {

  private static Integer PAGE_SIZE = 10000;
  private Query query;
  private OLuceneIndexEngine engine;
  private OLuceneQueryContext queryContext;
  private String indexName;
  private Highlighter highlighter;
  private List<String> highlighted;
  private int maxNumFragments;
  private TopDocs topDocs;
  private long deletedMatchCount = 0;

  private boolean closed = false;

  protected OLuceneResultSet() {}

  public OLuceneResultSet(
      final OLuceneIndexEngine engine,
      final OLuceneQueryContext queryContext,
      final ODocument metadata) {
    this.engine = engine;
    this.queryContext = queryContext;
    this.query = queryContext.getQuery();
    this.indexName = engine.indexName();

    fetchFirstBatch();
    deletedMatchCount = calculateDeletedMatch();

    final Map<String, Object> highlight =
        Optional.ofNullable(metadata.<Map>getProperty("highlight")).orElse(Collections.emptyMap());

    highlighted =
        Optional.ofNullable((List<String>) highlight.get("fields")).orElse(Collections.emptyList());

    final String startElement = (String) Optional.ofNullable(highlight.get("start")).orElse("<B>");

    final String endElement = (String) Optional.ofNullable(highlight.get("end")).orElse("</B>");

    final Scorer scorer = new QueryTermScorer(queryContext.getQuery());
    final Formatter formatter = new SimpleHTMLFormatter(startElement, endElement);
    highlighter = new Highlighter(formatter, scorer);

    maxNumFragments = (int) Optional.ofNullable(highlight.get("maxNumFragments")).orElse(2);
  }

  protected void fetchFirstBatch() {
    try {
      final IndexSearcher searcher = queryContext.getSearcher();
      if (queryContext.getSort() == null) {
        topDocs = searcher.search(query, PAGE_SIZE);
      } else {
        topDocs = searcher.search(query, PAGE_SIZE, queryContext.getSort());
      }
    } catch (final IOException e) {
      OLogManager.instance()
          .error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
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

  public void sendLookupTime(OCommandContext commandContext, long start) {
    OLuceneIndexEngineUtils.sendLookupTime(indexName, commandContext, topDocs, -1, start);
  }

  protected long calculateDeletedMatch() {
    return queryContext.deletedDocs(query);
  }

  @Override
  public int size() {
    return (int) Math.max(0, topDocs.totalHits - deletedMatchCount);
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLuceneResultSetIteratorTx();
  }

  private class OLuceneResultSetIteratorTx implements Iterator<OIdentifiable> {

    private ScoreDoc[] scoreDocs;
    private int index;
    private int localIndex;
    private long totalHits;

    public OLuceneResultSetIteratorTx() {
      totalHits = topDocs.totalHits;
      index = 0;
      localIndex = 0;
      scoreDocs = topDocs.scoreDocs;
      OLuceneIndexEngineUtils.sendTotalHits(
          indexName, queryContext.getContext(), topDocs.totalHits - deletedMatchCount);
    }

    @Override
    public boolean hasNext() {
      final boolean hasNext = index < (totalHits - deletedMatchCount);
      if (!hasNext && !closed) {
        final IndexSearcher searcher = queryContext.getSearcher();
        if (searcher.getIndexReader().getRefCount() > 1) {
          engine.release(searcher);
          closed = true;
        }
      }
      return hasNext;
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
      if (localIndex == scoreDocs.length) {
        localIndex = 0;
        fetchMoreResult();
      }
      final ScoreDoc score = scoreDocs[localIndex++];
      return score;
    }

    private Document toDocument(final ScoreDoc score) {
      try {
        return queryContext.getSearcher().doc(score.doc);
      } catch (final IOException e) {
        OLogManager.instance().error(this, "Error during conversion to document", e);
        return null;
      }
    }

    private OContextualRecordId toRecordId(final Document doc, final ScoreDoc score) {
      final String rId = doc.get(OLuceneIndexEngineAbstract.RID);
      final OContextualRecordId res = new OContextualRecordId(rId);

      final IndexReader indexReader = queryContext.getSearcher().getIndexReader();
      try {
        for (final String field : highlighted) {
          final String text = doc.get(field);
          if (text != null) {
            TokenStream tokenStream =
                TokenSources.getAnyTokenStream(
                    indexReader, score.doc, field, doc, engine.indexAnalyzer());
            TextFragment[] frag =
                highlighter.getBestTextFragments(tokenStream, text, true, maxNumFragments);
            queryContext.addHighlightFragment(field, frag);
          }
        }
        engine.onRecordAddedToResultSet(queryContext, res, doc, score);
        return res;
      } catch (IOException | InvalidTokenOffsetsException e) {
        throw OException.wrapException(new OLuceneIndexException("error while highlighting"), e);
      }
    }

    private boolean isToSkip(final OContextualRecordId recordId, final Document doc) {
      return isDeleted(recordId, doc) || isUpdatedDiskMatch(recordId, doc);
    }

    private void fetchMoreResult() {
      TopDocs topDocs = null;
      try {
        final IndexSearcher searcher = queryContext.getSearcher();
        if (queryContext.getSort() == null) {
          topDocs = searcher.searchAfter(scoreDocs[scoreDocs.length - 1], query, PAGE_SIZE);
        } else {
          topDocs =
              searcher.searchAfter(
                  scoreDocs[scoreDocs.length - 1], query, PAGE_SIZE, queryContext.getSort());
        }
        scoreDocs = topDocs.scoreDocs;
      } catch (final IOException e) {
        OLogManager.instance()
            .error(this, "Error on fetching document by query '%s' to Lucene index", e, query);
      }
    }

    private boolean isDeleted(OIdentifiable value, Document doc) {
      return queryContext.isDeleted(doc, null, value);
    }

    private boolean isUpdatedDiskMatch(OIdentifiable value, Document doc) {
      return isUpdated(value) && !isTempMatch(doc);
    }

    private boolean isUpdated(OIdentifiable value) {
      return queryContext.isUpdated(null, null, value);
    }

    private boolean isTempMatch(Document doc) {
      return doc.get(OLuceneTxChangesAbstract.TMP) != null;
    }

    @Override
    public void remove() {
      // TODO: something to be done here?
    }
  }
}
