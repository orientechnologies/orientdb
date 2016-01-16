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

import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class BinaryTest extends DocumentDBBaseTest {
  private ORID rid;

  @Parameters(value = "url")
  public BinaryTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testMixedCreateEmbedded() {
    ODocument doc = new ODocument();
    doc.field("binary", "Binary data".getBytes());

    doc.save();

    doc.reload();
    Assert.assertEquals(new String((byte[]) doc.field("binary", OType.BINARY)), "Binary data");
  }

  @Test
  public void testBasicCreateExternal() {
    ORecordBytes record = new ORecordBytes(database, "This is a test".getBytes());
    record.save();
    rid = record.getIdentity();
  }

  @Test(dependsOnMethods = "testBasicCreateExternal")
  public void testBasicReadExternal() {
    ORecordBytes record = database.load(rid);

    Assert.assertEquals("This is a test", new String(record.toStream()));
  }

  @Test(dependsOnMethods = "testBasicReadExternal")
  public void testMixedCreateExternal() {
    ODocument doc = new ODocument();
    doc.field("binary", new ORecordBytes(database, "Binary data".getBytes()));

    doc.save();
    rid = doc.getIdentity();
  }

  @Test(dependsOnMethods = "testMixedCreateExternal")
  public void testMixedReadExternal() {
    ODocument doc = new ODocument(rid);
    doc.reload();

    Assert.assertEquals("Binary data", new String(((ORecordBytes) doc.field("binary")).toStream()));
  }
}
