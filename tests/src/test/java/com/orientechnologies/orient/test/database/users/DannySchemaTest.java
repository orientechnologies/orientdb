package com.orientechnologies.orient.test.database.users;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DannySchemaTest {
  private ODatabaseDocumentTx db;
  private OClass dependents;
  private OClass master;

  @SuppressWarnings("unchecked")
  @Test
  public void test1() {

    db =
        new ODatabaseDocumentTx(
            "plocal:C:/work/dev/orientechnologies/orientdb/temp/danny/library/library");

    try {
      db.create();

      master = db.getMetadata().getSchema().createClass("Master");
      master.createProperty("type", OType.STRING);
      master.createProperty("master", OType.LONG);

      dependents = db.getMetadata().getSchema().createClass("Dependents");
      dependents.createProperty("type", OType.STRING);
      dependents.createProperty("dependents", OType.EMBEDDEDLIST, master);

      db.close();

      db.open("admin", "admin");
      dependents = db.getMetadata().getSchema().getClass("Dependents");
      master = db.getMetadata().getSchema().getClass("Master");

      // CREATE NEW DOC
      new ODocument("Dependents")
          .field(
              "dependents",
              new ODocument[] {
                new ODocument("Master")
                    .field("mastertype", "Title")
                    .field("master", 4151788013272153098L)
              })
          .save();
      db.close();

      db.open("admin", "admin");

      // LOAD IT AND CHECK THE LONG VALUE
      for (ODocument doc : db.browseClass("Dependents")) {
        System.out.println(doc);
        for (ODocument emb : (Iterable<ODocument>) doc.field("dependents"))
          Assert.assertEquals(emb.<Object>field("master"), 4151788013272153098L);
      }
      db.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
