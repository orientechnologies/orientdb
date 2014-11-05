package com.orientechnologies.orient.test.io;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test
public class MassiveConnectDisconnectTest {

  public static void main(String[] args) {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost/demo");
    for (int i = 0; i < 1000; ++i) {
      System.out.println("Connecting " + i + "...");
      db.open("admin", "admin");
      db.query(new OSQLSynchQuery<Object>("select 1"));
//      try {
//        Thread.sleep(100);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }
      db.close();
    }
  }

}
