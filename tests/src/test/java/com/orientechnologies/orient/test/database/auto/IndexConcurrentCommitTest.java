package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexConcurrentCommitTest extends DocumentDBBaseTest {
  @Parameters(value = "url")
  public IndexConcurrentCommitTest(@Optional String url) {
    super(url);
  }

  public void testConcurrentUpdate() {
    OClass personClass = database.getMetadata().getSchema().createClass("Person");
    personClass.createProperty("ssn", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    try {
      // Transaction 1
      database.begin();

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
      database.commit();

      // Ensure that the people made it in correctly
      final OResultSet result1 = database.query("select from Person");
      System.out.println("After transaction 1");
      while (result1.hasNext()) System.out.println(result1.next());

      // Transaction 2
      database.begin();

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
      database.commit();

      System.out.println("Success!");
    } catch (OIndexException e) {
      System.out.println("Exception: " + e.toString());
      database.rollback();
    }

    final OResultSet result2 = database.command("select from Person");
    System.out.println("After transaction 2");
    while (result2.hasNext()) System.out.println(result2.next());
  }
}
