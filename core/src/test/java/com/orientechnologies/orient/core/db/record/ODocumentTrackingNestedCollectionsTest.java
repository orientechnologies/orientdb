package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 11/03/16.
 */
public class ODocumentTrackingNestedCollectionsTest {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + ODocumentTrackingNestedCollectionsTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.close();
  }

  @Test
  public void testTrackingNestedList() {

    ORID orid;
    ODocument document = new ODocument();
    Set objects = new HashSet();

    document.field("objects", objects);
    document.save();

    objects = document.field("objects");
    Set subObjects = new HashSet();
    objects.add(subObjects);

    document.save();

    orid = document.getIdentity();
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    ODocument nestedDoc = new ODocument();
    subObjects.add(nestedDoc);

    document.save();
    db.getLocalCache().clear();

    document = db.load(orid);
    objects = document.field("objects");
    subObjects = (Set) objects.iterator().next();

    assertTrue(!subObjects.isEmpty());

  }

}
