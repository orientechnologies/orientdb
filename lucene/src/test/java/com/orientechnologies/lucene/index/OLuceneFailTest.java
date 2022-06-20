package com.orientechnologies.lucene.index;

import com.orientechnologies.orient.core.db.ODatabaseSession;
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
    odb.execute("create database tdb memory users (admin identified by 'admpwd' role admin)")
        .close();
  }

  @After
  public void after() {
    odb.close();
  }

  @Test
  public void test() {
    try (ODatabaseSession session = odb.open("tdb", "admin", "admpwd")) {
      session.command("create property V.text string").close();
      session.command("create index lucene_index on V(text) FULLTEXT ENGINE LUCENE").close();
      try {
        session.query("select from V where search_class('*this breaks') = true").close();
      } catch (Exception e) {
      }
      session.query("select from V ").close();
    }
  }
}
