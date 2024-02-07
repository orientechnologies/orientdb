package com.orientechnologies.orient.test.database.users;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import org.testng.annotations.Test;

public class SecMaskTest {
  private static ODatabaseDocument database;

  @Test
  public static void main(String[] args) {
    database = new ODatabaseDocumentTx("plocal:/tmp/secmask/secmask");
    if (database.exists()) database.open("admin", "admin");
    else {
      database.create();
      create();
    }

    // insert();
    query();
  }

  public static void insert() {
    database.declareIntent(new OIntentMassiveInsert());
    database.begin(TXTYPE.NOTX);
    long ndoc = 1000000;
    ODocument doc = new ODocument();

    System.out.println("Inserting " + ndoc + " docs...");

    long block = System.nanoTime();
    for (long i = 1; i <= ndoc; i++) {
      doc.field("id", i);
      doc.field("val1", 4.0d);
      doc.field("val2", 5.0d);
      doc.field("val3", 6.0f);
      doc.field("val4", 255);
      doc.field("val5", "this is the description for a long comic books -" + i);
      doc.field("name", "this is secmask put on top - " + i);
      doc.setClassName("Account");
      doc.save();
      doc.reset();
      if (i % 100000 == 0) {
        double time = (double) (System.nanoTime() - block) / 1000000;
        System.out.println(i * 100 / ndoc + "%.\t" + time + "\t" + 100000.0d / time + " docs/ms");
        block = System.nanoTime();
      }
    }
    database.commit();

    System.out.println("Insertion done, now indexing ids...");

    block = System.nanoTime();

    // CREATE THE INDEX AT THE END
    database
        .getMetadata()
        .getSchema()
        .getClass("Account")
        .getProperty("id")
        .createIndex(OClass.INDEX_TYPE.UNIQUE);

    System.out.println("Indexing done in: " + (System.nanoTime() - block) / 1000000 + "ms");
  }

  public static void query() {
    System.out.println("Querying docs...");

    // List<ODocument> result = database.query(new ONativeSynchQuery<ODocument,
    // OQueryContextNativeSchema<ODocument>>(database,
    // "Account", new OQueryContextNativeSchema<ODocument>()) {
    // @Override
    // public boolean filter(OQueryContextNativeSchema<ODocument> iRecord) {
    // return iRecord.field("id").eq(1000l).field("name").go();
    // }
    // });

    long start = System.currentTimeMillis();

    OResultSet result = database.query("SELECT FROM Account WHERE id = " + 100999);

    System.out.println("Elapsed: " + (System.currentTimeMillis() - start));

    System.out.println("Query done");

    while (result.hasNext()) {
      OResult o = result.next();
      System.out.println("id=" + o.getProperty("id") + "\tname=" + o.getProperty("name"));
    }
  }

  public static void create() {
    OClass account = database.getMetadata().getSchema().createClass("Account", 1, null);
    account.createProperty("id", OType.LONG);
    account.createProperty("val1", OType.DOUBLE);
    account.createProperty("val2", OType.DOUBLE);
    account.createProperty("val3", OType.FLOAT);
    account.createProperty("val4", OType.SHORT);
    account.createProperty("val5", OType.STRING);
    account.createProperty("name", OType.STRING);
  }
}
