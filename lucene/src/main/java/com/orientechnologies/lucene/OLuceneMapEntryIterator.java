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

package com.orientechnologies.lucene;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class OLuceneMapEntryIterator<K, V> implements Iterator<Map.Entry<K, V>> {

  private final OIndexDefinition definition;
  private IndexReader            reader;

  private int                    currentIdx;

  public OLuceneMapEntryIterator(IndexReader reader, OIndexDefinition definition) {

    this.reader = reader;
    this.definition = definition;
    this.currentIdx = 0;
  }

  @Override
  public boolean hasNext() {
    return currentIdx < reader.maxDoc();
  }

  @Override
  public Map.Entry<K, V> next() {
    try {
      Document doc = reader.document(currentIdx);
      String val = "";
      if (definition.getFields().size() > 0) {
        for (String field : definition.getFields()) {
          val += doc.get(field);
        }
      } else {
        val = doc.get(OLuceneIndexManagerAbstract.KEY);
      }
      final String finalVal = val;
      final ORecordId id = new ORecordId(doc.get(OLuceneIndexManagerAbstract.RID));
      currentIdx++;
      return new Map.Entry<K, V>() {
        @Override
        public K getKey() {
          return (K) finalVal;
        }

        @Override
        public V getValue() {
          return (V) id;
        }

        @Override
        public V setValue(V value) {
          return null;
        }
      };
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error on iterating Lucene result", e);
    }
    return null;
  }

  @Override
  public void remove() {

  }
}
