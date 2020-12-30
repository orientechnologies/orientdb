/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.tests;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import java.io.IOException;
import java.io.InputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/** Created by enricorisa on 19/09/14. */
public abstract class OLuceneBaseTest {

  @Rule public TestName name = new TestName();

  protected ODatabaseDocumentInternal db;

  protected OrientDB orient;
  protected ODatabasePool pool;

  @Before
  public void setupDatabase() {
    final String config =
        System.getProperty("orientdb.test.env", ODatabaseType.MEMORY.name().toLowerCase());
    setupDatabase(config);
  }

  protected void setupDatabase(String config) {
    OrientDBConfig cfg =
        OrientDBConfig.builder().addAttribute(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8).build();

    if ("ci".equals(config) || "release".equals(config)) {
      orient = new OrientDB("embedded:./target/databases/", cfg);
      if (orient.exists(name.getMethodName())) orient.drop(name.getMethodName());

      orient.execute(
          "create database ? plocal users(admin identified by 'admin' role admin) ",
          name.getMethodName());

    } else {
      orient = new OrientDB("embedded:", cfg);
      if (orient.exists(name.getMethodName())) orient.drop(name.getMethodName());

      orient.execute(
          "create database ? memory users(admin identified by 'admin' role admin) ",
          name.getMethodName());
    }

    pool = new ODatabasePool(orient, name.getMethodName(), "admin", "admin");
    db = (ODatabaseDocumentInternal) pool.acquire();
  }

  @After
  public void dropDatabase() {

    db.activateOnCurrentThread();
    db.close();
    pool.close();
    orient.drop(name.getMethodName());
    orient.close();
  }

  protected String getScriptFromStream(InputStream in) {
    try {
      return OIOUtils.readStreamAsString(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
