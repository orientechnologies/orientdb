package com.orientechnologies.orient.core.metadata;

import com.orientechnologies.orient.core.cache.OGlobalRecordCache;
import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;

public interface OMetadataInternal extends OMetadata {

  void makeThreadLocalSchemaSnapshot();

  void clearThreadLocalSchemaSnapshot();

  OImmutableSchema getImmutableSchemaSnapshot();

  void replaceGlobalCache(OGlobalRecordCache iGlobalCache);
}
