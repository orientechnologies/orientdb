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
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.engine.OLuceneIndexEngineAbstract;
import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Created by Enrico Risa on 16/09/15.
 */
public abstract class OLuceneAbstractResultSet implements Set<OIdentifiable> {

  protected static Integer PAGE_SIZE = 10000;
  protected final String             indexName;
  protected       TopDocs            topDocs;
  protected       Query              query;
  protected       OLuceneIndexEngine engine;
  protected       QueryContext       queryContext;

  public OLuceneAbstractResultSet(OLuceneIndexEngine engine, QueryContext queryContext) {
    this.engine = engine;
    this.queryContext = queryContext;
    this.query = enhanceQuery(queryContext.query);

    indexName = engine.indexName();
    fetchFirstBatch();
  }

  protected Query enhanceQuery(Query query) {
    return query;
  }

  protected void fetchFirstBatch() {
    try {

      switch (queryContext.cfg) {

      case NO_FILTER_NO_SORT:
        topDocs = queryContext.getSearcher().search(query, PAGE_SIZE);
        break;
      case FILTER_SORT:
        topDocs = queryContext.getSearcher().search(query, PAGE_SIZE, queryContext.sort);
        break;
      case FILTER:
        topDocs = queryContext.getSearcher().search(query, PAGE_SIZE);
        break;
      case SORT:
        topDocs = queryContext.getSearcher().search(query, PAGE_SIZE, queryContext.sort);
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
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
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
    OLuceneIndexEngineAbstract.sendLookupTime(indexName, commandContext, topDocs, -1, start);
  }
}
