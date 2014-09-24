package com.orientechnologies.orient.core.record;

public class ORecordListenerManager {

  public static void addListener(ORecordAbstract record, ORecordListener listener) {
    record.addListener(listener);
  }

  public static void removeListener(ORecordAbstract record, ORecordListener listener) {
    record.removeListener(listener);
  }

  
}
