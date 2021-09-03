/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.testng.annotations.Test;

public class ReadAllClusterObjectsSpeedTest extends SpeedTestMonoThread {
  private static final String CLASS_NAME = "Account";
  private ODatabaseDocumentTx db;
  private int objectsRead;
  private String url;

  public ReadAllClusterObjectsSpeedTest() {
    super(5);
    url = System.getProperty("url");
    if (url == null) throw new IllegalArgumentException("URL missing");
  }

  @Override
  @Test(enabled = false)
  public void init() throws IOException {
    db = new ODatabaseDocumentTx(url);
    db.open("admin", "admin");
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws UnsupportedEncodingException {
    objectsRead = 0;

    for (ODocument rec : db.browseClass(CLASS_NAME)) {
      ++objectsRead;
    }
  }

  @Override
  public void afterCycle() throws Exception {
    System.out.println(
        data.getCyclesDone()
            + "-> Read "
            + objectsRead
            + " objects in the cluster "
            + CLASS_NAME
            + "="
            + data().takeTimer());
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws IOException {
    System.out.println("Read " + objectsRead + " objects in the cluster " + CLASS_NAME);
    db.close();
  }
}
