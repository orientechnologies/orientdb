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

package com.orientechnologies.lucene.engine;

import com.orientechnologies.lucene.query.QueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;

/**
 * Created by Enrico Risa on 04/09/15.
 */
public interface OLuceneIndexEngine extends OIndexEngine {

  public void initIndex(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic,
      ODocument metadata);

  public String indexName();

  public abstract void onRecordAddedToResultSet(QueryContext queryContext, OContextualRecordId recordId, Document ret,
      ScoreDoc score);

  public Document buildDocument(Object key, OIdentifiable value);

  public Query buildQuery(Object query);

  //  public Analyzer analyzer(String field);

  public Analyzer indexAnalyzer();

  public Analyzer queryAnalyzer();

  public boolean remove(Object key, OIdentifiable value);

  public IndexSearcher searcher() throws IOException;

  public Object getInTx(Object key, OLuceneTxChanges changes);

  public long sizeInTx(OLuceneTxChanges changes);

  public OLuceneTxChanges buildTxChanges() throws IOException;

  public Query deleteQuery(Object key, OIdentifiable value);

}
