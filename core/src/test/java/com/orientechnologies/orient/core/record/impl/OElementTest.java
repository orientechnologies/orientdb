package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OElement;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;

/**
 * Created by luigidellaquila on 02/07/16.
 */

public class OElementTest {

  static ODatabase db;


  @BeforeClass
  public static void beforeClass(){
    db = new ODatabaseDocumentTx("memory:OElementTest");
    db.create();
  }
  @AfterClass
  public static void afterClass(){
    db.drop();
  }

  @Test
  public void testGetSetProperty(){
    OElement elem = db.newElement();
    elem.setProperty("foo", "foo1");
    elem.setProperty("foo.bar", "foobar");
    elem.setProperty("  ", "spaces");

    Set<String> names = elem.getPropertyNames();
    Assert.assertTrue(names.contains("foo"));
    Assert.assertTrue(names.contains("foo.bar"));
    Assert.assertTrue(names.contains("  "));
  }

  @Test
  public void testLoadAndSave(){

    OElement elem = db.newElement("Foo");
    elem.setProperty("foo", "foo1");
    db.save(elem);


    elem = (OElement) db.load(elem.getIdentity());
    Assert.assertEquals(elem.getProperty("foo"), "foo1");
  }
}
