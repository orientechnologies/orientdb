package com.orientechnologies.orient.core.record.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ORecordElement.STATUS;
import com.orientechnologies.orient.core.record.ORecord;

public class DirtyFinder {

  public static Set<ORecord> findDirty(ODocument document) {
    Set<ORecord> all = new HashSet<ORecord>();
    checkObject(document, all);
    return all;
  }

  public static Set<ORecord> findDirties(Iterable<ORecord> records) {
    Set<ORecord> all = new HashSet<ORecord>();
    for (ORecord oRecord : all) {
      checkObject(oRecord, all);
    }
    return all;
  }

  public static void findDirties(ORecord record, Set<ORecord> allLocks) {
    checkObject(record, allLocks);
  }

  @SuppressWarnings("unchecked")
  private static void checkObject(Object object, Set<ORecord> all) {
    if (object instanceof ODocument) {
      ODocument doc = (ODocument) object;
      if (all.contains(doc))
        return;
      if (doc.isDirty()) {
        if (!doc.isEmbedded())
          all.add(doc);
        STATUS prev = doc.getInternalStatus();
        doc.setInternalStatus(STATUS.MARSHALLING);
        for (Entry<String, Object> entry : (ODocument) object) {
          checkObject(entry.getValue(), all);
        }
        doc.setInternalStatus(prev);
      }
    } else if (object instanceof Iterable<?>) {
      for (Object elem : (Iterable<Object>) object) {
        checkObject(elem, all);
      }
    } else if (object instanceof Map<?, ?>) {
      for (Object elem : ((Map<Object, Object>) object).values()) {
        checkObject(elem, all);
      }
    } else if (object instanceof ORecord) {
      if (((ORecord) object).isDirty())
        all.add((ORecord) object);
    }

  }

}
