package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import java.util.ArrayList;

public class ORecordSerializationDebug {

  public String                                       className;
  public ArrayList<ORecordSerializationDebugProperty> properties;
  public boolean                                      readingFailure;
  public RuntimeException                             readingException;
  public int                                          failPosition;

}
