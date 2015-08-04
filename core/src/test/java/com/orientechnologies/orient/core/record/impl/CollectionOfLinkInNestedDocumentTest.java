package com.orientechnologies.orient.core.record.impl;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.Collections;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.metadata.schema.OType;

public class CollectionOfLinkInNestedDocumentTest {

  private ODatabaseDocument db;

  @BeforeMethod
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + CollectionOfLinkInNestedDocumentTest.class);
    db.create();
  }

  @AfterMethod
  public void after() {
    try {
      db.drop();
    } finally {

    }
  }

  @Test
  public void nestedLinkSet() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    ORecordLazySet set = new ORecordLazySet(nested);
    set.add(doc1);
    set.add(doc2);

    nested.field("set", set);

    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base);
    db.getLocalCache().clear();
    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);

    assertTrue(nested.field("set").equals(nest1.field("set")));
  }

  @Test
  public void nestedLinkList() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    ORecordLazyList list = new ORecordLazyList(nested);
    list.add(doc1);
    list.add(doc2);

    nested.field("list", list);

    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base);
    db.getLocalCache().clear();
    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(nest1.field("list"), nested.field("list"));
  }

  @Test
  public void nestedLinkMap() {
    ODocument doc1 = new ODocument();
    doc1.field("value", "item 1");
    ODocument doc2 = new ODocument();
    doc2.field("value", "item 2");
    ODocument nested = new ODocument();
    ORecordLazyMap map = new ORecordLazyMap(nested);
    map.put("record1", doc1);
    map.put("record2", doc2);

    nested.field("map", map);

    ODocument base = new ODocument();
    base.field("nested", nested, OType.EMBEDDED);
    OIdentifiable id = db.save(base);
    db.getLocalCache().clear();
    ODocument base1 = db.load(id.getIdentity());
    ODocument nest1 = base1.field("nested");
    assertNotNull(nest1);
    assertEquals(nest1.field("map"), nested.field("map"));
  }
}
