package com.orientechnologies.orient.server.distributed.impl.metadata;

import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OPropertyEmbedded;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by tglman on 22/06/17.
 */
public class OPropertyDistributed extends OPropertyEmbedded {
  public OPropertyDistributed(OClassImpl owner) {
    super(owner);
  }

  public OPropertyDistributed(OClassImpl owner, ODocument document) {
    super(owner, document);
  }

  public OPropertyDistributed(OClassImpl oClassImpl, OGlobalProperty global) {
    super(oClassImpl, global);
  }
}
