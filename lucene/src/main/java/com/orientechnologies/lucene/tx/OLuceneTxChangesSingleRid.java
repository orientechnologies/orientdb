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

package com.orientechnologies.lucene.tx;

import com.orientechnologies.lucene.OLuceneIndexType;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Enrico Risa on 15/09/15.
 */
public class OLuceneTxChangesSingleRid extends OLuceneTxChangesAbstract {

  private final Set<String>   deleted     = new HashSet<String>();
  private final Set<String>   updated     = new HashSet<String>();
  private final Set<Document> deletedDocs = new HashSet<Document>();

  public OLuceneTxChangesSingleRid(OLuceneIndexEngine engine, IndexWriter writer) {
    super(engine, writer);
  }

  public void put(Object key, OIdentifiable value, Document doc) throws IOException {
    if (deleted.remove(value.getIdentity().toString())) {
      doc.add(OLuceneIndexType.createField(TMP, value.getIdentity().toString(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS));
      updated.add(value.getIdentity().toString());
    }
    writer.addDocument(doc);
  }

  public void remove(Object key, OIdentifiable value) throws IOException {

    if (value.getIdentity().isTemporary()) {
      writer.deleteDocuments(engine.deleteQuery(key, value));
    } else {
      deleted.add(value.getIdentity().toString());
      deletedDocs.add(engine.buildDocument(key, value));
    }
  }

  public long numDocs() {
    return searcher().getIndexReader().numDocs() - deleted.size() - updated.size();
  }

  public Set<Document> getDeletedDocs() {
    return deletedDocs;
  }

  public boolean isDeleted(Document document, Object key, OIdentifiable value) {
    return deleted.contains(value.getIdentity().toString());
  }

  public boolean isUpdated(Document document, Object key, OIdentifiable value) {
    return updated.contains(value.getIdentity().toString());
  }

}
