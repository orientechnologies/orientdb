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

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "security")
public class RestrictedTest {
  private ODatabaseDocumentTx database;
  private ODocument           adminRecord;
  private ODocument           writerRecord;

  @Parameters(value = "url")
  public RestrictedTest(String iURL) {
    database = new ODatabaseDocumentTx(iURL);
  }

  @AfterMethod
  protected void closeDb() {
    database.close();
  }

  @Test
  public void testCreateRestrictedClass() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("CMSDocument", database.getMetadata().getSchema().getClass("ORestricted"));
    adminRecord = new ODocument("CMSDocument").field("user", "admin").save();
    adminRecord.reload();
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredQuery() throws IOException {
    database.open("writer", "writer");

    List<?> result = database.query(new OSQLSynchQuery<Object>("select from CMSDocument"));
    Assert.assertTrue(result.isEmpty());
  }

  @Test(dependsOnMethods = "testFilteredQuery")
  public void testCreateAsWriter() throws IOException {
    database.open("writer", "writer");
    writerRecord = new ODocument("CMSDocument").field("user", "writer").save();
    writerRecord.reload();
  }

  @Test(dependsOnMethods = "testCreateAsWriter")
  public void testFilteredQueryAsWriter() throws IOException {
    database.open("writer", "writer");
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>("select from CMSDocument"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredDirectReadAsWriter() throws IOException {
    database.open("writer", "writer");
    Assert.assertNull(database.load(adminRecord));
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredDirectUpdateAsWriter() throws IOException {
    database.open("writer", "writer");
    adminRecord.field("user", "writer-hacker");
    try {
      adminRecord.save();
    } catch (OSecurityException e) {
      // OK AS EXCEPTION
    } catch (ORecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    database.close();

    database.open("admin", "admin");
    Assert.assertEquals(((ODocument) adminRecord.reload()).field("user"), "admin");
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredDirectDeleteAsWriter() throws IOException {
    database.open("writer", "writer");
    try {
      adminRecord.delete();
    } catch (OSecurityException e) {
      // OK AS EXCEPTION
    } catch (ORecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    database.close();

    database.open("admin", "admin");
    adminRecord.reload();
    Assert.assertEquals(adminRecord.field("user"), "admin");
  }

  @Test(dependsOnMethods = "testCreateRestrictedClass")
  public void testFilteredHackingAllowFieldAsWriter() throws IOException {
    database.open("writer", "writer");
    try {
      // FORCE LOADING
      Set<OIdentifiable> allows = adminRecord.field(OSecurityShared.ALLOW_FIELD);
      allows.add(database.getMetadata().getSecurity().getUser(database.getUser().getName()).getDocument().getIdentity());
      adminRecord.save();
    } catch (OSecurityException e) {
      // OK AS EXCEPTION
    } catch (ORecordNotFoundException e) {
      // OK AS EXCEPTION
    }
    database.close();

    database.open("admin", "admin");
    adminRecord.reload();
  }

  @Test(dependsOnMethods = "testCreateAsWriter")
  public void testAddAdminAsRole() throws IOException {
    database.open("writer", "writer");
    Set<OIdentifiable> allows = ((ODocument) writerRecord.reload()).field(OSecurityShared.ALLOW_FIELD);
    allows.add(database.getMetadata().getSecurity().getRole("admin").getDocument().getIdentity());
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testAddAdminAsRole")
  public void testAdminCanSeeWriterDocumentAfterPermission() throws IOException {
    database.open("admin", "admin");
    Assert.assertNotNull(database.load(writerRecord));
  }
}
