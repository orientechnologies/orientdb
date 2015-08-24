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
import com.orientechnologies.orient.core.exception.OSecurityException;
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

    try {
      ORole writerGrandChild = security.createRole("writerGrandChild", writerChild, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
      writerGrandChild.save();

      try {
        OUser child = security.createUser("writerChild", "writerChild", writerGrandChild);
        child.save();

        try {
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
          database.open("admin", "admin");
        } finally {
          security.dropUser("writerChild");
        }
      } finally {
        security.dropRole("writerGrandChild");
      }
    } finally {
      security.dropRole("writerChild");
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
      database.open("admin", "admin");
      security.dropUser("noRole");
    }
  }

  @Test
  public void testAdminCanSeeSystemClusters() {
    database.open("admin", "admin");

    List<ODocument> result = database.command(new OCommandSQL("select from ouser")).execute();
    Assert.assertFalse(result.isEmpty());

    Assert.assertTrue(database.browseClass("OUser").hasNext());

    Assert.assertTrue(database.browseCluster("OUser").hasNext());
  }

  @Test
  public void testOnlyAdminCanSeeSystemClusters() {
    database.open("reader", "reader");

    try {
      database.command(new OCommandSQL("select from ouser")).execute();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseClass("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseCluster("OUser").hasNext());
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  @Test
  public void testCannotExtendClassWithNoUpdateProvileges() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");
    database.close();

    database.open("writer", "writer");

    try {
      database.command(new OCommandSQL("alter class Protected superclass OUser")).execute();
      Assert.fail();
    } catch (OSecurityException e) {
    } finally {
      database.close();

      database.open("admin", "admin");
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testSuperUserCanExtendClassWithNoUpdateProvileges() {
    database.open("admin", "admin");
    database.getMetadata().getSchema().createClass("Protected");

    try {
      database.command(new OCommandSQL("alter class Protected superclass OUser")).execute();
    } finally {
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }
}
