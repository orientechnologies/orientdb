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
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.parser.ParseException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.OIndexEngineCallback;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

public class OLuceneFullTextIndex extends OLuceneIndexNotUnique implements OLuceneIndex {

  public OLuceneFullTextIndex(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata);
  }

  public Document buildDocument(final Object key) {

    while (true)
      try {
        return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Document>() {
          @Override
          public Document callEngine(OIndexEngine engine) {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            return indexEngine.buildDocument(key, null);
          }
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  public Query buildQuery(final Object query) throws ParseException {
    while (true)
      try {
        return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Query>() {
          @Override
          public Query callEngine(OIndexEngine engine) {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            return indexEngine.buildQuery(query);
          }
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }

  }

  public Analyzer queryAnalyzer() {
    while (true)
      try {
        return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Analyzer>() {
          @Override
          public Analyzer callEngine(OIndexEngine engine) {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            return indexEngine.queryAnalyzer();
          }
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
  }

  public Analyzer indexAnalyzer() {
    while (true) {
      try {
        return storage.callIndexEngine(false, false, indexId, new OIndexEngineCallback<Analyzer>() {
          @Override
          public Analyzer callEngine(OIndexEngine engine) {
            OLuceneIndexEngine indexEngine = (OLuceneIndexEngine) engine;
            return indexEngine.indexAnalyzer();
          }
        });
      } catch (OInvalidIndexEngineIdException e) {
        doReloadIndexEngine();
      }
    }
  }
}
