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
import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.lucene.exception.OLuceneIndexException;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;

/** Created by Enrico Risa on 15/09/15. */
public class OLuceneTxChangesMultiRid extends OLuceneTxChangesAbstract {
  private final Map<String, List<String>> deleted = new HashMap<String, List<String>>();
  private final Set<Document> deletedDocs = new HashSet<Document>();

  public OLuceneTxChangesMultiRid(
      final OLuceneIndexEngine engine, final IndexWriter writer, final IndexWriter deletedIdx) {
    super(engine, writer, deletedIdx);
  }

  public void put(final Object key, final OIdentifiable value, final Document doc) {
    try {
      writer.addDocument(doc);
    } catch (IOException e) {
      throw OException.wrapException(
          new OLuceneIndexException("unable to add document to changes index"), e);
    }
  }

  public void remove(final Object key, final OIdentifiable value) {
    try {
      if (value.getIdentity().isTemporary()) {
        writer.deleteDocuments(engine.deleteQuery(key, value));
      } else {
        deleted.putIfAbsent(value.getIdentity().toString(), new ArrayList<>());
        deleted.get(value.getIdentity().toString()).add(key.toString());

        final Document doc = engine.buildDocument(key, value);
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
    return searcher().getIndexReader().numDocs() - deletedDocs.size();
  }

  public Set<Document> getDeletedDocs() {
    return deletedDocs;
  }

  public boolean isDeleted(final Document document, final Object key, final OIdentifiable value) {
    boolean match = false;
    final List<String> strings = deleted.get(value.getIdentity().toString());
    if (strings != null) {
      final MemoryIndex memoryIndex = new MemoryIndex();
      for (final String string : strings) {
        final Query q = engine.deleteQuery(string, value);
        memoryIndex.reset();
        for (final IndexableField field : document.getFields()) {
          memoryIndex.addField(field.name(), field.stringValue(), new KeywordAnalyzer());
        }
        match = match || (memoryIndex.search(q) > 0.0f);
      }
      return match;
    }
    return match;
  }

  // TODO is this valid?
  public boolean isUpdated(final Document document, final Object key, final OIdentifiable value) {
    return false;
  }
}
