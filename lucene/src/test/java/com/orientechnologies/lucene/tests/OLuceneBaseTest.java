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
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by enricorisa on 19/09/14.
 */
public abstract class OLuceneBaseTest {

  @Rule
  public TestName name = new TestName();

  protected ODatabaseDocument db;

  protected OrientDB      orient;
  protected ODatabasePool pool;

  @Before
  public void setupDatabase() {

    String config = System.getProperty("orientdb.test.env", "memory");

    setupDatabase(config);

  }

  protected void setupDatabase(String config) {
    if ("ci".equals(config) || "release".equals(config)) {
      orient = new OrientDB("embedded:./target/databases/", OrientDBConfig.defaultConfig());
      if (orient.exists(name.getMethodName()))
        orient.drop(name.getMethodName());

      orient.create(name.getMethodName(), ODatabaseType.PLOCAL);

    } else {
      orient = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
      if (orient.exists(name.getMethodName()))
        orient.drop(name.getMethodName());

      orient.create(name.getMethodName(), ODatabaseType.MEMORY);

    }

    pool = new ODatabasePool(orient, name.getMethodName(), "admin", "admin");
    db = pool.acquire();
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
