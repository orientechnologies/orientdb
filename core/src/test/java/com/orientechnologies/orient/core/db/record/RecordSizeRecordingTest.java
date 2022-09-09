package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import org.junit.Test;

public class RecordSizeRecordingTest {

  @Test
  public void testRecordSize() {
    try (OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database test memory users (admin identified by 'admin' role admin)");

      try (ODatabaseSession session = orientDB.open("test", "admin", "admin")) {
        long size = session.getSize();
        assertNotEquals(0, size);
        session.save(new ORecordBytes(new byte[10]));
        assertEquals(session.getSize(), size + 10);
      }
    }
  }
}
