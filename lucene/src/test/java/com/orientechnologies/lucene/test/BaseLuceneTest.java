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

package com.orientechnologies.lucene.test;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by enricorisa on 19/09/14.
 */
public abstract class BaseLuceneTest {

  @Rule
  public TestName name = new TestName();

  protected ODatabaseDocumentInternal db;
  protected OrientDB                  context;

  protected ODatabaseType type;

  @Before
  public void setupDatabase() throws Throwable {

    String config = System.getProperty("orientdb.test.env", "memory");

    String path;

    if ("ci".equals(config) || "release".equals(config)) {

      type = ODatabaseType.PLOCAL;

      path = "embedded:./target/databases";

    } else {
      type = ODatabaseType.MEMORY;
      path = "embedded:.";

    }
    context = new OrientDB(path, OrientDBConfig.defaultConfig());

    if (context.exists(name.getMethodName())) {
      context.drop(name.getMethodName());
    }

    context.create(name.getMethodName(), type);

    db = (ODatabaseDocumentInternal) context.open(name.getMethodName(), "admin", "admin");
    db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8);

  }

  public ODatabaseSession openDatabase() {
    return context.open(name.getMethodName(), "admin", "admin");
  }

  public void createDatabase() {
    context.create(name.getMethodName(), type);
  }

  @After
  public void dropDatabase() {
    db.activateOnCurrentThread();

    context.drop(name.getMethodName());
  }

  protected ODatabaseDocumentTx dropOrCreate(String url, boolean drop) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (db.exists()) {
      db.open("admin", "admin");
      if (drop) {
        // DROP AND RE-CREATE IT
        db.drop();
        db = new ODatabaseDocumentTx(url);
        db.create();
      }
    } else {
      // CREATE IT
      db = new ODatabaseDocumentTx(url);
      db.create();

    }

    db.activateOnCurrentThread();

    return db;
  }

  protected String getScriptFromStream(InputStream in) {
    try {
      return OIOUtils.readStreamAsString(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
