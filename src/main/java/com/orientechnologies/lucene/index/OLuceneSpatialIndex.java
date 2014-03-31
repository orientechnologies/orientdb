package com.orientechnologies.lucene.index;

import com.orientechnologies.lucene.OLuceneIndexEngine;

/**
 * Created by enricorisa on 28/03/14.
 */
public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  public OLuceneSpatialIndex(String typeId, String algorithm, OLuceneIndexEngine engine, String valueContainerAlgorithm) {
    super(typeId, algorithm, engine, valueContainerAlgorithm);
  }
}
