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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.lucene.builder.OLuceneIndexType;
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

/** Created by Enrico Risa on 15/09/15. */
public class OLuceneTxChangesSingleRid extends OLuceneTxChangesAbstract {
  private final Set<String> deleted = new HashSet<String>();
  private final Set<String> updated = new HashSet<String>();
  private final Set<Document> deletedDocs = new HashSet<Document>();

  public OLuceneTxChangesSingleRid(
      final OLuceneIndexEngine engine, final IndexWriter writer, final IndexWriter deletedIdx) {
    super(engine, writer, deletedIdx);
  }

  public void put(final Object key, final OIdentifiable value, final Document doc) {
    if (deleted.remove(value.getIdentity().toString())) {
      doc.add(OLuceneIndexType.createField(TMP, value.getIdentity().toString(), Field.Store.YES));
      updated.add(value.getIdentity().toString());
    }
    try {
      writer.addDocument(doc);
    } catch (IOException e) {
      throw OException.wrapException(
          new OLuceneIndexException("unable to add document to changes index"), e);
    }
  }

  public void remove(final Object key, final OIdentifiable value) {
    try {
      if (value == null) {
        writer.deleteDocuments(engine.deleteQuery(key, value));
      } else if (value.getIdentity().isTemporary()) {
        writer.deleteDocuments(engine.deleteQuery(key, value));
      } else {
        deleted.add(value.getIdentity().toString());
        Document doc = engine.buildDocument(key, value);
        deletedDocs.add(doc);
        deletedIdx.addDocument(doc);
      }
    } catch (final IOException e) {
      throw OException.wrapException(
          new OLuceneIndexException(
              "Error while deleting documents in transaction from lucene index"),
          e);
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
