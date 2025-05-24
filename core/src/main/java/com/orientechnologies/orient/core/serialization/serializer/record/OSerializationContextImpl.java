package com.orientechnologies.orient.core.serialization.serializer.record;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;

public class OSerializationContextImpl implements OSerializationContext {

  private ODatabaseDocumentInternal database;

  public OSerializationContextImpl() {
    this.database = ODatabaseRecordThreadLocal.instance().getIfDefined();
  }

  @Override
  public OSBTreeCollectionManager getCollectionManager() {
    if (database != null) {
      return database.getSbTreeCollectionManager();
    } else {
      return null;
    }
  }

  @Override
  public OImmutableSchema getImmutableSchema() {
    if (database != null) {
      return database.getMetadata().getImmutableSchemaSnapshot();
    } else {
      return null;
    }
  }
}
