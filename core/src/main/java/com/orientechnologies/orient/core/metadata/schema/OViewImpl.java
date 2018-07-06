package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.record.impl.ODocument;

public abstract class OViewImpl extends OClassImpl implements OView {
  protected OViewImpl(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  protected OViewImpl(OSchemaShared iOwner, String iName, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
  }

  protected OViewImpl(OSchemaShared iOwner, ODocument iDocument, String iName) {
    super(iOwner, iDocument, iName);
  }
}
