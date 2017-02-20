package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.object.db.entity.SimpleChild;
import com.orientechnologies.orient.object.db.entity.SimpleParent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 17/02/17.
 */
public class SimpleParentChildTest {

  private ODatabaseObject database;

  @Before
  public void before() {
    database = new OObjectDatabaseTx("memory:" + SimpleParentChildTest.class.getSimpleName());
    database.create();
    database.getEntityManager().registerEntityClass(SimpleChild.class);
    database.getEntityManager().registerEntityClass(SimpleParent.class);
  }

  @After
  public void after() {
    database.drop();
  }

  @Test
  public void testParentChild() {
    SimpleChild sc = new SimpleChild();
    sc.setName("aa");
    SimpleParent sa = new SimpleParent();
    sa.setChild(sc);
    SimpleParent ret = database.save(sa);
    database.getLocalCache().clear();
    ODocument doc = ((OObjectDatabaseTx) database).getUnderlying().load(ret.getId().getIdentity());
    assertEquals(doc.fieldType("child"), OType.LINK);
  }

}
