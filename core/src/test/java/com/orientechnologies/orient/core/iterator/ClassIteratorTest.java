package com.orientechnologies.orient.core.iterator;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.ODefaultClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage.LOCKING_STRATEGY;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Artem Loginov
 */
@Test
public class ClassIteratorTest {
  private static final boolean       RECREATE_DATABASE = true;
  private static ODatabaseDocumentTx db                = null;
  private Set<String>                names;

  @BeforeMethod
  public void setUp() throws Exception {
    initializeDatabase();

    // Insert some data
    names = new HashSet<String>();
    names.add("Adam");
    names.add("Bob");
    names.add("Calvin");
    names.add("Daniel");

    for (String name : names) {
      createPerson(name);
    }
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (!db.isClosed())
      db.close();
  }

  @Test
  public void testIteratorShouldReuseRecordWithoutNPE() {

    // Use class iterator.
    // browseClass() returns all documents in RecordID order
    // (including subclasses, which shouldn't exist for Person)
    final ORecordIteratorClass<ODocument> personIter = db.browseClass("Person");

    // Setting this to true causes the bug. Setting to false it works fine.
    personIter.setReuseSameRecord(true);

    int docNum = 0;
    // Explicit iterator loop.
    while (personIter.hasNext()) {
      final ODocument personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc.toString());
    }

    Assert.assertTrue(names.isEmpty());
  }

  @Test
  public void testIteratorShouldReuseRecordWithoutNPEUsingForEach() throws Exception {
    // Use class iterator.
    // browseClass() returns all documents in RecordID order
    // (including subclasses, which shouldn't exist for Person)
    final ORecordIteratorClass<ODocument> personIter = db.browseClass("Person");

    // Setting this to true causes the bug. Setting to false it works fine.
    personIter.setReuseSameRecord(true);

    // Shorthand iterator loop.
    int docNum = 0;
    for (final ODocument personDoc : personIter) {
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));

      System.out.printf("Doc %d: %s\n", docNum++, personDoc.toString());
    }

    Assert.assertTrue(names.isEmpty());
  }

  @Test
  public void testDescendentOrderIteratorWithMultipleClusters() throws Exception {
    final OClass personClass = db.getMetadata().getSchema().getClass("Person");

    // empty old cluster but keep it attached
    personClass.truncate();

    // reload the data in a new 'test' cluster
    int testClusterId = db.addCluster("test");
    personClass.addClusterId(testClusterId);
    personClass.setClusterSelection(new ODefaultClusterSelectionStrategy());
    personClass.setDefaultClusterId(testClusterId);

    for (String name : names) {
      createPerson(name);
    }

    // Use descending class iterator.
    final ORecordIteratorClass<ODocument> personIter =
        new ORecordIteratorClassDescendentOrder<ODocument>(
db, db, "Person", true,
        false, LOCKING_STRATEGY.DEFAULT);

    personIter.setRange(null, null); // open range

    int docNum = 0;
    // Explicit iterator loop.
    while (personIter.hasNext()) {
      final ODocument personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc.toString());
    }

    Assert.assertTrue(names.isEmpty());
  }

  private static void initializeDatabase() {
    db = new ODatabaseDocumentTx("memory:" + ClassIteratorTest.class.getSimpleName());
    if (db.exists() && RECREATE_DATABASE) {
      db.open("admin", "admin");
      db.drop();
      System.out.println("Dropped database.");
    }
    if (!db.exists()) {
      db.create();
      System.out.println("Created database.");

      final OSchema schema = db.getMetadata().getSchema();

      // Create Person class
      final OClass personClass = schema.createClass("Person");
      personClass.createProperty("First", OType.STRING).setMandatory(true).setNotNull(true).setMin("1");

      System.out.println("Created schema.");
    } else {
      db.open("admin", "admin");
    }
  }

  private static void createPerson(final String first) {
    // Create Person document
    final ODocument personDoc = db.newInstance("Person");
    personDoc.field("First", first);
    personDoc.save();
  }
}
