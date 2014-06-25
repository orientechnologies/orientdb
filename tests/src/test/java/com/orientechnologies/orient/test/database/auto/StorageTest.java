/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.test.database.auto;

import java.io.File;
import java.io.IOException;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.ODataSegmentStrategy;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class StorageTest {
  private String              url;
  private ODatabaseDocumentTx database;

  @Parameters(value = "url")
  public StorageTest(String iURL) {
    url = iURL;
  }

  @Test
  public void testCreateDataSegment() throws IOException {
    if (url.startsWith("plocal:"))
      return;

    database = new ODatabaseDocumentTx(url);
    if (!ODatabaseHelper.existsDatabase(database, "plocal"))
      ODatabaseHelper.createDatabase(database, url, "plocal");

    database.open("admin", "admin");

    File tempDir = new File(System.getProperty("java.io.tmpdir") + "/binary-Segment");
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    final int segmentId = database.addDataSegment("binary", tempDir.toString());

    database.setDataSegmentStrategy(new ODataSegmentStrategy() {

      @Override
      public int assignDataSegmentId(ODatabase iDatabase, ORecord<?> iRecord) {
        return 1;
      }
    });

    ODocument record = database.newInstance().field("name", "data-segment-test").save();
    Assert.assertNotNull(record);

    record.delete();

    database.dropDataSegment("binary");
    try {
      database.getDataSegmentIdByName("binary");
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }

    final int newSegmentId = database.addDataSegment("binary", tempDir.toString());
    Assert.assertEquals(segmentId, newSegmentId);

    database.dropDataSegment("binary");
    try {
      database.getDataSegmentIdByName("binary");
      Assert.assertTrue(false);
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(true);
    }
  }
}
