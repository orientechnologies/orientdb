package com.orientechnologies.lucene.manager;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Created by enricorisa on 31/03/14.
 */
public class OLuceneDictionaryIndexManager extends OLuceneUniqueIndexManager {

  @Override
  public void put(Object key, Object value) {
    remove(key, (OIdentifiable) value);
    super.put(key, value);
  }
}
