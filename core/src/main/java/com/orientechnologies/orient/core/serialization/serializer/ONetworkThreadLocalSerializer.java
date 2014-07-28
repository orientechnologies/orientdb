package com.orientechnologies.orient.core.serialization.serializer;

import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

public class ONetworkThreadLocalSerializer {
  private static final ThreadLocal<ORecordSerializer> networkSerializer = new ThreadLocal<ORecordSerializer>();

  public static ORecordSerializer getNetworkSerializer() {
    return networkSerializer.get();
  }

  public static void setNetworkSerializer(ORecordSerializer value) {
    networkSerializer.set(value);
  }
}
