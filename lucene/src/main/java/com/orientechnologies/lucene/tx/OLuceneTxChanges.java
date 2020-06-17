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

package com.orientechnologies.lucene.tx;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Collections;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

/** Created by Enrico Risa on 15/09/15. */
public interface OLuceneTxChanges {

  void put(Object key, OIdentifiable value, Document doc);

  void remove(Object key, OIdentifiable value);

  IndexSearcher searcher();

  default long numDocs() {
    return 0;
  }

  default Set<Document> getDeletedDocs() {
    return Collections.emptySet();
  }

  boolean isDeleted(Document document, Object key, OIdentifiable value);

  boolean isUpdated(Document document, Object key, OIdentifiable value);

  default long deletedDocs(Query query) {
    return 0;
  }
}
