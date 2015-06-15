package com.orientechnologies.orient.core.metadata;

import com.orientechnologies.orient.core.metadata.schema.OImmutableSchema;

public interface OMetadataInternal  extends OMetadata{

  public void makeThreadLocalSchemaSnapshot();

  public void clearThreadLocalSchemaSnapshot();

  public OImmutableSchema getImmutableSchemaSnapshot();
  
}
