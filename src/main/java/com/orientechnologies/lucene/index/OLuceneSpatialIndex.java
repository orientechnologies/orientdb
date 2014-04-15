package com.orientechnologies.lucene.index;

import com.orientechnologies.lucene.OLuceneIndexEngine;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  public OLuceneSpatialIndex(String typeId, String algorithm, OLuceneIndexEngine engine, String valueContainerAlgorithm) {
    super(typeId, algorithm, engine, valueContainerAlgorithm);
  }

  @Override
  protected void populateIndex(ODocument doc, Object fieldValue) {
    put(fieldValue, doc);
  }

  @Override
  protected Object getCollatingValue(Object key) {
    return key;
  }
}
