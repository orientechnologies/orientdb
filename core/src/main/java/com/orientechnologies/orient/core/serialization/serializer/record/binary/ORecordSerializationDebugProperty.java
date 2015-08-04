package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;

public class ORecordSerializationDebugProperty {

  public String           name;
  public int              globalId;
  public OType            type;
  public RuntimeException readingException;
  public boolean          faildToRead;
  public int              failPosition;
  public Object           value;

}
