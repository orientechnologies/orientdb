package com.orientechnologies.orient.core.serialization.serializer.record;

import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;

public interface OSerializationContext {

  OSBTreeCollectionManager getCollectionManager();

  OImmutableSchema getImmutableSchema();
}
