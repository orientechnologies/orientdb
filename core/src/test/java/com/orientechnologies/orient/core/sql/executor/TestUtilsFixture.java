package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/** Created by olena.kolesnyk on 28/07/2017. */
public class TestUtilsFixture {
  protected static ODatabaseDocument database;
  protected static OrientDB factory;
  private static final String PATH = "memory";
  private static final String DB_NAME = "test_database";
  private static final String USER = "admin";
  private static final String PASSWORD = OCreateDatabaseUtil.NEW_ADMIN_PASSWORD;

  @BeforeClass
  public static void setUp() {
    factory = OCreateDatabaseUtil.createDatabase(DB_NAME, PATH, OCreateDatabaseUtil.TYPE_PLOCAL);
    database = factory.open(DB_NAME, USER, PASSWORD);
  }

  @AfterClass
  public static void tearDown() {
    database.close();
    factory.drop(DB_NAME);
    factory.close();
  }

  static OClass createClassInstance() {
    return getDBSchema().createClass(generateClassName());
  }

  static OClass createChildClassInstance(OClass superclass) {
    return getDBSchema().createClass(generateClassName(), superclass);
  }

  private static OSchema getDBSchema() {
    return database.getMetadata().getSchema();
  }

  private static String generateClassName() {
    return "Class" + RandomStringUtils.randomNumeric(10);
  }
}
