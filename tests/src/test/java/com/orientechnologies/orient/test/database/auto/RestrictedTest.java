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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurityShared;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Test(groups = "security")
public class RestrictedTest extends DocumentDBBaseTest {
  private ODocument adminRecord;
  private ODocument writerRecord;

  private OUser     readerUser = null;
  private ORole     readerRole = null;

  @Parameters(value = "url")
  public RestrictedTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testCreateRestrictedClass() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("CMSDocument", database.getMetadata().getSchema().getClass("ORestricted"));
    adminRecord = new ODocument("CMSDocument").field("user", "admin").save();
    adminRecord.reload();

    readerUser = database.getMetadata().getSecurity().getUser("reader");
    readerRole = database.getMetadata().getSecurity().getRole("reader");
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
  public void testFilteredQueryAsReader() throws IOException {
    database.open("reader", "reader");
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>("select from CMSDocument"));
    Assert.assertEquals(result.size(), 0);
  }

  @Test(dependsOnMethods = "testFilteredQueryAsReader")
  public void testFilteredQueryAsAdmin() throws IOException {
    database.open("admin", "admin");
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>("select from CMSDocument where user = 'writer'"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testFilteredQueryAsAdmin")
  public void testFilteredQueryAsWriter() throws IOException {
    database.open("writer", "writer");
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<Object>("select from CMSDocument"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testFilteredQueryAsWriter")
  public void testFilteredDirectReadAsWriter() throws IOException {
    database.open("writer", "writer");
    Assert.assertNull(database.load(adminRecord));
  }

  @Test(dependsOnMethods = "testFilteredDirectReadAsWriter")
  public void testFilteredDirectUpdateAsWriter() throws IOException {
    database.open("writer", "writer");
    adminRecord.field("user", "writer-hacker");
    try {
      adminRecord.save();
    } catch (OSecurityException e) {
      // OK AS EXCEPTION
    } catch (ORecordNotFoundException e) {
      // OK AS EXCEPTION
    } catch (OResponseProcessingException e) {
      final Throwable t = e.getCause();

      Assert.assertTrue(t instanceof OSecurityException || t instanceof ORecordNotFoundException);
    }
    database.close();

    database.open("admin", "admin");
    Assert.assertEquals(((ODocument) database.load(adminRecord.getIdentity())).field("user"), "admin");
  }

  @Test(dependsOnMethods = "testFilteredDirectUpdateAsWriter")
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

  @Test(dependsOnMethods = "testFilteredDirectDeleteAsWriter")
  public void testFilteredHackingAllowFieldAsWriter() throws IOException {
    database.open("writer", "writer");
    try {
      // FORCE LOADING
      Set<OIdentifiable> allows = adminRecord.field(OSecurityShared.ALLOW_ALL_FIELD);
      allows.add(database.getMetadata().getSecurity().getUser(database.getUser().getName()).getDocument().getIdentity());
      adminRecord.save();
    } catch (OSecurityException e) {
      // OK AS EXCEPTION
    } catch (ORecordNotFoundException e) {
      // OK AS EXCEPTION
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityException || e.getCause() instanceof ORecordNotFoundException);
    }
    database.close();

    database.open("admin", "admin");
    adminRecord.reload();
  }

  @Test(dependsOnMethods = "testFilteredHackingAllowFieldAsWriter")
  public void testAddReaderAsRole() throws IOException {
    database.open("writer", "writer");
    Set<OIdentifiable> allows = ((ODocument) writerRecord.reload()).field(OSecurityShared.ALLOW_ALL_FIELD);
    allows.add(readerRole.getIdentity());
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testAddReaderAsRole")
  public void testReaderCanSeeWriterDocumentAfterPermission() throws IOException {
    database.open("reader", "reader");
    Assert.assertNotNull(database.load(writerRecord));
  }

  @Test(dependsOnMethods = "testReaderCanSeeWriterDocumentAfterPermission")
  public void testWriterRoleCanRemoveReader() throws IOException {
    database.open("writer", "writer");
    Assert.assertEquals(((Collection<?>) writerRecord.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 2);
    database.getMetadata().getSecurity().disallowRole(writerRecord, OSecurityShared.ALLOW_ALL_FIELD, "reader");
    Assert.assertEquals(((Collection<?>) writerRecord.field(OSecurityShared.ALLOW_ALL_FIELD)).size(), 1);
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testWriterRoleCanRemoveReader")
  public void testReaderCannotSeeWriterDocument() throws IOException {
    database.open("reader", "reader");
    Assert.assertNull(database.load(writerRecord.getIdentity()));
  }

  @Test(dependsOnMethods = "testReaderCannotSeeWriterDocument")
  public void testWriterAddReaderUserOnlyForRead() throws IOException {
    database.open("writer", "writer");
    database.getMetadata().getSecurity().allowUser(writerRecord, OSecurityShared.ALLOW_READ_FIELD, "reader");
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testWriterAddReaderUserOnlyForRead")
  public void testReaderCanSeeWriterDocument() throws IOException {
    database.open("reader", "reader");
    Assert.assertNotNull(database.load(writerRecord.getIdentity()));
  }

  /***** TESTS FOR #1980: Record Level Security: permissions don't follow role's inheritance *****/
  @Test(dependsOnMethods = "testReaderCanSeeWriterDocument")
  public void testWriterRemoveReaderUserOnlyForRead() throws IOException {
    database.open("writer", "writer");
    database.getMetadata().getSecurity().disallowUser(writerRecord, OSecurityShared.ALLOW_READ_FIELD, "reader");
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testWriterRemoveReaderUserOnlyForRead")
  public void testReaderCannotSeeWriterDocumentAgain() throws IOException {
    database.open("reader", "reader");
    Assert.assertNull(database.load(writerRecord.getIdentity()));
  }

  @Test(dependsOnMethods = "testReaderCannotSeeWriterDocumentAgain")
  public void testReaderRoleInheritsFromWriterRole() throws IOException {
    database.open("admin", "admin");
    ORole reader = database.getMetadata().getSecurity().getRole("reader");
    reader.setParentRole(database.getMetadata().getSecurity().getRole("writer"));
    reader.save();
  }

  @Test(dependsOnMethods = "testReaderRoleInheritsFromWriterRole")
  public void testWriterRoleCanSeeWriterDocument() throws IOException {
    database.open("writer", "writer");
    database.getMetadata().getSecurity().allowRole(writerRecord, OSecurityShared.ALLOW_READ_FIELD, "writer");
    writerRecord.save();
  }

  @Test(dependsOnMethods = "testWriterRoleCanSeeWriterDocument")
  public void testReaderRoleCanSeeInheritedDocument() {
    database.open("reader", "reader");
    Assert.assertNotNull(database.load(writerRecord.getIdentity()));
  }

  @Test(dependsOnMethods = "testReaderRoleCanSeeInheritedDocument")
  public void testReaderRoleDesntInheritsFromWriterRole() throws IOException {
    database.open("admin", "admin");
    ORole reader = database.getMetadata().getSecurity().getRole("reader");
    reader.setParentRole(null);
    reader.save();
  }

  /**** END TEST FOR #1980: Record Level Security: permissions don't follow role's inheritance ****/

  @Test(dependsOnMethods = "testReaderRoleDesntInheritsFromWriterRole")
  public void testTruncateClass() {
    database.open("admin", "admin");
    try {
      database.command(new OCommandSQL("truncate class CMSDocument")).execute();
      Assert.fail();
    } catch (OSecurityException e) {
      Assert.assertTrue(true);
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityException);
    }

  }

  @Test(dependsOnMethods = "testTruncateClass")
  public void testTruncateUnderlyingCluster() {
    database.open("admin", "admin");
    try {
      database.command(new OCommandSQL("truncate cluster CMSDocument")).execute();
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityException);
    } catch (OSecurityException e) {

    }

  }

  @Test(dependsOnMethods = "testTruncateUnderlyingCluster")
  public void testUpdateRestricted() {
    database.open("admin", "admin");
    database.getMetadata().getSchema()
        .createClass("TestUpdateRestricted", database.getMetadata().getSchema().getClass("ORestricted"));
    adminRecord = new ODocument("TestUpdateRestricted").field("user", "admin").save();

    database.close();

    database.open("writer", "writer");
    List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select from TestUpdateRestricted"));
    Assert.assertTrue(result.isEmpty());

    database.close();

    database.open("admin", "admin");
    database.command(new OCommandSQL("update TestUpdateRestricted content {\"data\":\"My Test\"}")).execute();
    result = database.query(new OSQLSynchQuery<ODocument>("select from TestUpdateRestricted"));

    Assert.assertEquals(result.size(), 1);

    final ODocument doc = result.get(0);
    Assert.assertEquals(doc.field("data"), "My Test");
    doc.field("user", "admin");
    doc.save();
    database.close();

    database.open("writer", "writer");
    result = database.query(new OSQLSynchQuery<Object>("select from TestUpdateRestricted"));
    Assert.assertTrue(result.isEmpty());
  }

  @BeforeMethod
  protected void closeDb() {
    database.close();
  }
}
