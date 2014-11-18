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

import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Enrico Risa on 28/10/14.
 */
public class LuceneResultSet implements Set<OIdentifiable> {

  private final TopDocs       docs;
  private final IndexSearcher searcher;
  private ScoreDoc[]          hits;

  public LuceneResultSet(IndexSearcher searcher, TopDocs docs) {
    this.searcher = searcher;
    this.docs = docs;
    this.hits = docs.scoreDocs;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public boolean contains(Object o) {
    return false;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new OLuceneResultSetIterator(docs);
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

    public OLuceneResultSetIterator(TopDocs docs) {

      array = docs.scoreDocs;
    }

    @Override
    public boolean hasNext() {
      return index < array.length;
    }

    @Override
    public OIdentifiable next() {
      final ScoreDoc score = hits[index++];
      Document ret = null;
      OContextualRecordId res = null;
      try {
        ret = searcher.doc(score.doc);
        String rId = ret.get(OLuceneIndexManagerAbstract.RID);
        res = new OContextualRecordId(rId).setContext(new HashMap<String, Object>() {
          {
            put("score", score.score);
          }
        });
      } catch (IOException e) {
        e.printStackTrace();
      }

      return res;
    }

    @Override
    public void remove() {

    }
  }
}
