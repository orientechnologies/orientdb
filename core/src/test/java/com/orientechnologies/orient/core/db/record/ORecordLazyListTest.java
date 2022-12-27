package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ORecordLazyListTest {
  private OrientDB orientDb;
  private ODatabaseSession dbSession;

  @Before
  public void init() throws Exception {
    orientDb =
        OCreateDatabaseUtil.createDatabase(
            ORecordLazyListTest.class.getSimpleName(), "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
    dbSession =
        orientDb.open(
            ORecordLazyListTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    OSchema schema = dbSession.getMetadata().getSchema();
    OClass mainClass = schema.createClass("MainClass");
    mainClass.createProperty("name", OType.STRING);
    OProperty itemsProp = mainClass.createProperty("items", OType.LINKLIST);
    OClass itemClass = schema.createClass("ItemClass");
    itemClass.createProperty("name", OType.STRING);
    itemsProp.setLinkedClass(itemClass);
    ODocument doc1 = new ODocument(itemClass).field("name", "Doc1").save();
    ODocument doc2 = new ODocument(itemClass).field("name", "Doc2").save();
    ODocument doc3 = new ODocument(itemClass).field("name", "Doc3").save();

    ODocument mainDoc = new ODocument(mainClass).field("name", "Main Doc");
    mainDoc.field("items", Arrays.asList(doc1, doc2, doc3));
    mainDoc.save();

    mainDoc = (ODocument) mainDoc.reload();
    Collection<ODocument> origItems = mainDoc.field("items");
    Iterator<ODocument> it = origItems.iterator();
    assertTrue(it.next() instanceof ODocument);
    assertTrue(it.next() instanceof ODocument);
    //  assertTrue(it.next() instanceof ODocument);

    List<ODocument> items = new ArrayList<ODocument>(origItems);
    assertTrue(items.get(0) instanceof ODocument);
    //  assertEquals(doc1, items.get(0));
    assertTrue(items.get(1) instanceof ODocument);
    //  assertEquals(doc2, items.get(1));
    assertTrue(items.get(2) instanceof ODocument);
    //  assertEquals(doc3, items.get(2));
  }

  @After
  public void close() {
    if (dbSession != null) {
      dbSession.close();
    }
    if (orientDb != null && dbSession != null) {
      orientDb.drop(ORecordLazyListTest.class.getSimpleName());
    }
  }
}
