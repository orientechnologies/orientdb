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

import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.query.QueryContext;

/**
 * Created by Enrico Risa on 16/09/15.
 */
public class LuceneResultSetFactory {

  public static LuceneResultSetFactory INSTANCE = new LuceneResultSetFactory();

  protected LuceneResultSetFactory() {
  }

  public OLuceneAbstractResultSet create(OLuceneIndexEngine manager, QueryContext queryContext) {

    if (queryContext.isInTx()) {
      return new OLuceneTxResultSet(manager, queryContext);
    } else {
      return new LuceneResultSet(manager, queryContext);

    }

  }
}
