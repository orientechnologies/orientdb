/*
 * Copyright 2014 Orient Technologies.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.lucene.index;

import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

public class OLuceneFullTextIndex extends OLuceneIndexNotUnique implements OLuceneIndex {

  public OLuceneFullTextIndex(String name, String typeId, String algorithm,int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm,version, storage, valueContainerAlgorithm, metadata);
  }

  public Document buildDocument(Object key) {

//    OLuceneIndexEngine luceneIndexEngine = (OLuceneIndexEngine) indexEngine;
//
//    return luceneIndexEngine.buildDocument(key);
    return null;
  }

  public Query buildQuery(Object query) throws ParseException {

//    OLuceneIndexEngine luceneIndexEngine = (OLuceneIndexEngine) indexEngine;
//    return luceneIndexEngine.buildQuery(query);
    return null;
  }

  public Analyzer analyzer(String field) {
//    OLuceneIndexEngine luceneIndexEngine = (OLuceneIndexEngine) indexEngine;
//    return luceneIndexEngine.analyzer(field);

    return null;
  }
}
