/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://orientdb.com
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.sql.DriverManager;
import java.util.Properties;

import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createSchemaDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

public abstract class OrientJdbcBaseTest {

  protected OrientJdbcConnection conn;
  protected ODatabaseDocumentTx  db;

  @Before
  public void prepareDatabase() throws Exception {
    String dbUrl = "memory:" + getClass().getSimpleName();
    db = new ODatabaseDocumentTx(dbUrl);

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
    db.activateOnCurrentThread();
    db.drop();

    //should reset the underlying pool because the db is dropped
    OrientJdbcConnection.POOL_FACTORY.reset();
  }
}
