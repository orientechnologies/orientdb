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

package com.orientechnologies.lucene.engine;

import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OContextualRecordId;
import com.orientechnologies.orient.core.index.engine.OIndexEngine;
import com.orientechnologies.orient.core.storage.impl.local.OFreezableStorageComponent;
import java.io.IOException;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

/** Created by Enrico Risa on 04/09/15. */
public interface OLuceneIndexEngine extends OIndexEngine, OFreezableStorageComponent {

  String indexName();

  void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext, OContextualRecordId recordId, Document ret, ScoreDoc score);

  Document buildDocument(Object key, OIdentifiable value);

  Query buildQuery(Object query);

  Analyzer indexAnalyzer();

  Analyzer queryAnalyzer();

  boolean remove(Object key, OIdentifiable value);

  boolean remove(Object key);

  IndexSearcher searcher();

  void release(IndexSearcher searcher);

  Set<OIdentifiable> getInTx(Object key, OLuceneTxChanges changes);

  long sizeInTx(OLuceneTxChanges changes);

  OLuceneTxChanges buildTxChanges() throws IOException;

  Query deleteQuery(Object key, OIdentifiable value);

  boolean isCollectionIndex();
}
