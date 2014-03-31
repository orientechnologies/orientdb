package com.orientechnologies.lucene.index;

import com.orientechnologies.lucene.OLuceneIndex;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndexEngine;
import com.orientechnologies.orient.core.index.OIndexUnique;

/**
 * Created by enricorisa on 26/03/14.
 */
public class OLuceneIndexUnique extends OIndexUnique implements OLuceneIndex {

  public OLuceneIndexUnique(String typeId, String algorithm, OIndexEngine<OIdentifiable> engine, String valueContainerAlgorithm) {
    super(typeId, algorithm, engine, valueContainerAlgorithm);
  }
}
