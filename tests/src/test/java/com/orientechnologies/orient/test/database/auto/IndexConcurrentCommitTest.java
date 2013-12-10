package com.orientechnologies.orient.test.database.auto;

import java.util.List;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test
public class IndexConcurrentCommitTest {
  public void testConcurrentUpdate() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("plocal:scratchpad");
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    OClass personClass = db.getMetadata().getSchema().createClass("Person");
    personClass.createProperty("ssn", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    try {
      // Transaction 1
      db.begin();

      // Insert two people in a transaction
      ODocument person1 = new ODocument("Person");
      person1.field("name", "John Doe");
      person1.field("ssn", "111-11-1111");
      person1.save();

      ODocument person2 = new ODocument("Person");
      person2.field("name", "Jane Doe");
      person2.field("ssn", "222-22-2222");
      person2.save();

      // Commit
      db.commit();

      // Ensure that the people made it in correctly
      final List<ODocument> result1 = db.command(new OCommandSQL("select from Person")).execute();
      System.out.println("After transaction 1");
      for (ODocument d : result1)
        System.out.println(d);

      // Transaction 2
      db.begin();

      // Update the ssn for the second person
      person2.field("ssn", "111-11-1111");
      person2.save();

      // Update the ssn for the first person
      person1.field("ssn", "222-22-2222");
      person1.save();

      System.out.println("To be committed:");
      System.out.println(person1);
      System.out.println(person2);
      // Commit - We get a transaction failure!
      db.commit();

      System.out.println("Success!");
    } catch (OIndexException e) {
      System.out.println("Exception: " + e.toString());
      db.rollback();
    }

    final List<ODocument> result2 = db.command(new OCommandSQL("select from Person")).execute();
    System.out.println("After transaction 2");
    for (ODocument d : result2)
      System.out.println(d);

    db.close();
  }
}
