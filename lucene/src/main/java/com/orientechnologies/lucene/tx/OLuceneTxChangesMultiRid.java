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

import com.orientechnologies.lucene.engine.OLuceneIndexEngine;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.*;

/**
 * Created by Enrico Risa on 15/09/15.
 */
public class OLuceneTxChangesMultiRid extends OLuceneTxChangesAbstract {
  private final Map<String, List<String>> deleted     = new HashMap<String, List<String>>();
  private final Set<Document>             deletedDocs = new HashSet<Document>();

  public OLuceneTxChangesMultiRid(OLuceneIndexEngine engine, IndexWriter writer, IndexWriter deletedIdx) {
    super(engine, writer, deletedIdx);
  }

  public void put(Object key, OIdentifiable value, Document doc) throws IOException {
    writer.addDocument(doc);
  }

  public void remove(Object key, OIdentifiable value) throws IOException {

    if (value.getIdentity().isTemporary()) {
      writer.deleteDocuments(engine.deleteQuery(key, value));
    } else {
      List<String> strings = deleted.get(value.getIdentity().toString());
      if (strings == null) {
        strings = new ArrayList<String>();
        deleted.put(value.getIdentity().toString(), strings);
      }
      strings.add(key.toString());
      Document doc = engine.buildDocument(key, value);
      deletedDocs.add(doc);
      deletedIdx.addDocument(doc);
    }
  }

  public long numDocs() {
    return searcher().getIndexReader().numDocs() - deletedDocs.size();
  }

  public Set<Document> getDeletedDocs() {
    return deletedDocs;
  }

  public boolean isDeleted(Document document, Object key, OIdentifiable value) {
    boolean match = false;
    List<String> strings = deleted.get(value.getIdentity().toString());
    if (strings != null) {
      MemoryIndex memoryIndex = new MemoryIndex();
      for (String string : strings) {
        Query q = engine.deleteQuery(string, value);
        memoryIndex.reset();
        for (IndexableField field : document.getFields()) {
          memoryIndex.addField(field.name(), field.stringValue(), new KeywordAnalyzer());
        }
        match = match || (memoryIndex.search(q) > 0.0f);
      }
      return match;
    }
    return match;
  }

  // TODO is this valid?
  public boolean isUpdated(Document document, Object key, OIdentifiable value) {
    return false;
  }

}
