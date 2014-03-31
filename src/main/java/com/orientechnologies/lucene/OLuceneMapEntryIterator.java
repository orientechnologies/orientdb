package com.orientechnologies.lucene;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.orientechnologies.lucene.manager.OLuceneIndexManagerAbstract;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexDefinition;

/**
 * Created by enricorisa on 21/03/14.
 */
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
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void remove() {

  }
}
