package com.orientechnologies.lucene.query;

import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by frank on 08/06/2017.
 */
public class OLuceneKeyAndMetadata {

  public final OLuceneCompositeKey key;
  public final ODocument           metadata;

  public OLuceneKeyAndMetadata(OLuceneCompositeKey key, ODocument metadata) {
    this.key = key;
    this.metadata = metadata;
  }

}
