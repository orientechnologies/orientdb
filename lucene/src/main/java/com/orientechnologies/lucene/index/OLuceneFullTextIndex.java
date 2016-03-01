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
import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;

public class OLuceneFullTextIndex extends OLuceneIndexNotUnique implements OLuceneIndex {

  public OLuceneFullTextIndex(String name, String typeId, String algorithm, OLuceneIndexEngine indexEngine,
      String valueContainerAlgorithm, ODocument metadata, OStorage storage) {
    super(name, typeId, algorithm, indexEngine, valueContainerAlgorithm, metadata, storage);
    indexEngine.setIndexMetadata(metadata);
  }

  public Document buildDocument(Object key) {
    return getIndexEngine().buildDocument(key, null);
  }

  public Analyzer analyzer(String name) {
    return getIndexEngine().analyzer(name);
  }

  public Query buildQuery(Object query) throws ParseException {
    return getIndexEngine().buildQuery(query);
  }
}
