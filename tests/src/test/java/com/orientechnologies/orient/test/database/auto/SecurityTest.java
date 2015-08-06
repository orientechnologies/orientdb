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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorageProxy;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Date;
import java.util.List;

@Test(groups = "security")
public class SecurityTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SecurityTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    database.close();
  }

  public void testWrongPassword() throws IOException {
    try {
      database.open("reader", "swdsds");
    } catch (OException e) {
      Assert.assertTrue(e instanceof OSecurityAccessException || e.getCause() != null
          && e.getCause().toString().indexOf("com.orientechnologies.orient.core.exception.OSecurityAccessException") > -1);
    }
  }

  public void testSecurityAccessWriter() throws IOException {
    database.open("writer", "writer");

    try {
      new ODocument().save("internal");
      Assert.assertTrue(false);
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityAccessException);
    } finally {
      database.close();
    }
  }

  @Test
  public void testSecurityAccessReader() throws IOException {
    database.open("reader", "reader");

    try {
      new ODocument("Profile").fields("nick", "error", "password", "I don't know", "lastAccessOn", new Date(), "registeredOn",
          new Date()).save();
    } catch (OSecurityAccessException e) {
      Assert.assertTrue(true);
    } catch (OResponseProcessingException e) {
      Assert.assertTrue(e.getCause() instanceof OSecurityAccessException);
    } finally {
      database.close();
    }
  }

  @Test
  public void testEncryptPassword() throws IOException {
    database.open("admin", "admin");

    Integer updated = database.command(new OCommandSQL("update ouser set password = 'test' where name = 'reader'")).execute();
    Assert.assertEquals(updated.intValue(), 1);

    List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select from ouser where name = 'reader'"));
    Assert.assertFalse(result.get(0).field("password").equals("test"));

    // RESET OLD PASSWORD
    updated = database.command(new OCommandSQL("update ouser set password = 'reader' where name = 'reader'")).execute();
    Assert.assertEquals(updated.intValue(), 1);

    result = database.query(new OSQLSynchQuery<Object>("select from ouser where name = 'reader'"));
    Assert.assertFalse(result.get(0).field("password").equals("reader"));

    database.close();
  }

  public void testParentRole() {
    database.open("admin", "admin");

    final OSecurity security = database.getMetadata().getSecurity();
    ORole writer = security.getRole("writer");

    ORole writerChild = security.createRole("writerChild", writer, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
    writerChild.save();

    ORole writerGrandChild = security.createRole("writerGrandChild", writerChild, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
    writerGrandChild.save();

    OUser child = security.createUser("writerChild", "writerChild", writerGrandChild);
    child.save();

    Assert.assertTrue(child.hasRole("writer", true));
    Assert.assertFalse(child.hasRole("wrter", true));

    database.close();
    if (!(database.getStorage() instanceof OStorageProxy)) {
      database.open("writerChild", "writerChild");

      OSecurityUser user = database.getUser();
      Assert.assertTrue(user.hasRole("writer", true));
      Assert.assertFalse(user.hasRole("wrter", true));

      database.close();
    }
  }

  @Test
  public void testQuotedUserName() {
    database.open("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    ORole adminRole = security.getRole("admin");
    OUser newUser = security.createUser("user'quoted", "foobar", adminRole);

    database.close();

    database.open("user'quoted", "foobar");
    database.close();

    database.open("admin", "admin");
    security = database.getMetadata().getSecurity();
    OUser user = security.getUser("user'quoted");
    Assert.assertNotNull(user);
    security.dropUser(user.getName());

    database.close();

    try {
      database.open("user'quoted", "foobar");
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testUserNoRole() {
    database.open("admin", "admin");

    OSecurity security = database.getMetadata().getSecurity();

    OUser newUser = security.createUser("noRole", "noRole", (String[]) null);

    database.close();

    try {
      database.open("noRole", "noRole");
      Assert.fail();
    } catch (OSecurityAccessException e) {
    }
  }
}
