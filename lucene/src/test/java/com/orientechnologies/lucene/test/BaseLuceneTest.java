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
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.io.IOException;
import java.io.InputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/** Created by enricorisa on 19/09/14. */
public abstract class BaseLuceneTest {
  @Rule public TestName name = new TestName();

  protected ODatabaseDocumentInternal db;
  protected OrientDB context;

  protected ODatabaseType type;

  @Before
  public void setupDatabase() throws Throwable {
    final String config =
        System.getProperty("orientdb.test.env", ODatabaseType.MEMORY.name().toLowerCase());
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
    context.execute(
        "create database ? " + type.toString() + " users(admin identified by 'admin' role admin) ",
        name.getMethodName());

    db = (ODatabaseDocumentInternal) context.open(name.getMethodName(), "admin", "admin");
    db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 8);
  }

  public ODatabaseSession openDatabase() {
    return context.open(name.getMethodName(), "admin", "admin");
  }

  public void createDatabase() {
    context.execute(
        "create database ? " + type + " users(admin identified by 'admin' role admin) ",
        name.getMethodName());
  }

  @After
  public void dropDatabase() {
    db.activateOnCurrentThread();
    context.drop(name.getMethodName());
  }

  protected ODatabaseDocumentTx dropOrCreate(final String url, final boolean drop) {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (db.exists()) {
      db.open("admin", "admin");
      if (drop) {
        db = dropAndCreateDocumentDatabase(url, db);
      }
    } else {
      db = createDocumentDatabase(url);
    }
    db.activateOnCurrentThread();
    return db;
  }

  private ODatabaseDocumentTx dropAndCreateDocumentDatabase(
      final String url, ODatabaseDocumentTx db) {
    db.drop();
    db = createDocumentDatabase(url);
    return db;
  }

  private ODatabaseDocumentTx createDocumentDatabase(final String url) {
    ODatabaseDocumentTx db;
    db = new ODatabaseDocumentTx(url);
    db.create();
    return db;
  }

  protected String getScriptFromStream(final InputStream scriptStream) {
    try {
      return OIOUtils.readStreamAsString(scriptStream);
    } catch (final IOException e) {
      throw new RuntimeException("Could not read script stream.", e);
    }
  }
}
