package com.orientechnologies.orient.jdbc;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.sql.DriverManager;
import java.util.Properties;

import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createSchemaDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;
import static java.lang.Class.forName;

public abstract class OrientJdbcBaseTest {

  protected OrientJdbcConnection conn;

  @BeforeClass
  public static void loadDriver() throws ClassNotFoundException {
//    forName(OrientJdbcDriver.class.getName());

  }

  @Before
  public void prepareDatabase() throws Exception {
    String dbUrl = "memory:test";
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);

    String username = "admin";
    String password = "admin";

    if (db.exists()) {
      db.activateOnCurrentThread();
      db.open(username, password);
      db.drop();
    }

    db.create();

    createSchemaDB(db);

    if (!new File("./src/test/resources/file.pdf").exists())
      OLogManager.instance().warn(this, "TEST IS NOT RUNNING UNDER distributed folder, attachment will be not loaded!");

    loadDB(db, 20);

    Properties info = new Properties();
    info.put("user", username);
    info.put("password", password);

    conn = (OrientJdbcConnection) DriverManager.getConnection("jdbc:orient:" + dbUrl, info);
  }

  @After
  public void closeConnection() throws Exception {
    if (conn != null && !conn.isClosed())
      conn.close();
  }
}
