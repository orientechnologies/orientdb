package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.testng.annotations.Test;

public class TestOrientBulkInsert {

  private static int NUM_INSERTLOOPS = 30000;

  @Test
  public static void main(String[] args) throws Exception {
    new TestOrientBulkInsert();
  }

  public TestOrientBulkInsert() throws InterruptedException {
    OGlobalConfiguration.INDEX_AUTO_LAZY_UPDATES.setValue(0); // Turn off cache
    OGlobalConfiguration.INDEX_MANUAL_LAZY_UPDATES.setValue(0);

    Map defaultsMap = new HashMap<String, Object>();
    defaultsMap.put("mvrbtree.lazyUpdates", 1);
    defaultsMap.put("index.auto.lazyUpdates", 1);
    defaultsMap.put("index.manual.lazyUpdates", 1);
    defaultsMap.put("index.auto.rebuildAfterNotSoftClose", false);

    OGlobalConfiguration.setConfiguration(defaultsMap);

    ODatabaseDocumentTx database = new ODatabaseDocumentTx("plocal:/temp/databases/bulktest");
    if (database.exists()) database.open("admin", "admin").drop();

    database.create();

    OSchema schema = database.getMetadata().getSchema();

    OClass cBulk = schema.createClass("classBulk", 1, null);

    // Declare some fields
    for (int i = 1; i < 10; i++) {
      cBulk.createProperty("fieldString" + i, OType.STRING).setMandatory(true);
    }
    OProperty p2 = cBulk.createProperty("fieldBinary1", OType.BINARY).setMandatory(false);

    // Declare some Indexes
    cBulk.createIndex("indexField1", INDEX_TYPE.NOTUNIQUE, "fieldString1");
    cBulk.createIndex("indexField2", INDEX_TYPE.UNIQUE, "fieldString2");
    cBulk.createIndex("indexField3", INDEX_TYPE.DICTIONARY, "fieldString3");

    database.declareIntent(new OIntentMassiveInsert());

    for (int i = 0; i < NUM_INSERTLOOPS; i++) {
      database.save(createRandomDocument());

      if (i % 1000 == 0) {
        System.out.println("Instert document:" + i);
      }

      // Slow duration of test down to 5 Minutes
      // Comment next line for faster execution
      // Thread.sleep(1000*60*5 / NUM_INSERTLOOPS);
    }

    database.close();

    do {
      // gives us time to analyse....
      System.gc();
      Thread.sleep(10000);
    } while (true);
  }

  private ODocument createRandomDocument() {
    ODocument document = new ODocument("classBulk");

    for (int i = 1; i < 10; i++) {
      document.field("fieldString" + i, getRandomText(50));
    }
    document.field("fieldBinary1", new byte[1024]);
    return document;
  }

  static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  static Random rnd = new Random();
  static int counter = 0;

  private String getRandomText(int len) {

    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(AB.charAt(rnd.nextInt(AB.length())));
    }
    String s = sb.toString() + " - " + (++counter);
    return s;
  }
}
