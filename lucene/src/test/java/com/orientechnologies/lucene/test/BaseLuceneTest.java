/*
 *
 *  * Copyright 2014 Orient Technologies.
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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by enricorisa on 19/09/14.
 */
public abstract class BaseLuceneTest {

  protected ODatabaseDocumentTx databaseDocumentTx;
  protected String              buildDirectory;
  private   String              url;

  public BaseLuceneTest() {
  }

  public void initDB() {
    initDB(true);
  }

  public void initDB(boolean drop) {
    String config = System.getProperty("orientdb.test.env");

    String storageType;
    if ("ci".equals(config) || "release".equals(config))
      storageType = OEngineLocalPaginated.NAME;
    else
      storageType = System.getProperty("storageType");

    if (storageType == null)
      storageType = OEngineMemory.NAME;

    buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    if (storageType.equals(OEngineLocalPaginated.NAME))
      url = OEngineLocalPaginated.NAME + ":" + "./target/databases/" + getDatabaseName();
    else
      url = OEngineMemory.NAME + ":" + getDatabaseName();

    databaseDocumentTx = dropOrCreate(url, drop);
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

  protected final String getDatabaseName() {
    return getClass().getSimpleName();
  }

  public void deInitDB() {
    databaseDocumentTx.drop();
  }

  protected String getScriptFromStream(InputStream in) {
    try {
      return OIOUtils.readStreamAsString(in);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
