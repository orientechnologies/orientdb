package com.orientechnologies.lucene.index;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OLuceneFailTest {
  private OrientDB odb;

  @Before
  public void before() {
    odb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    odb.create("tdb", ODatabaseType.MEMORY);
  }

  @After
  public void after() {
    odb.close();
  }

  @Test
  public void test() {
    try (ODatabaseSession session = odb.open("tdb", "admin", "admin")) {
      session.command("create property V.text string").close();
      session.command("create index lucene_index on V(text) FULLTEXT ENGINE LUCENE").close();
      try {
        session.query("select from V where search_class('*this breaks') = true").close();
      } catch (Exception e) {
      }
      long count = session.query("select from V ").stream().count();
      assertEquals(count, 0);
    }
  }
}
