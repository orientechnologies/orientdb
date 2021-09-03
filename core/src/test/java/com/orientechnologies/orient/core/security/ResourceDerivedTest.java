/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author SDIPro */
public class ResourceDerivedTest {

  private ODatabaseDocumentTx db;

  @Before
  public void before() {
    db = new ODatabaseDocumentTx("memory:" + ResourceDerivedTest.class.getSimpleName());

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    db.command(
        "CREATE SECURITY POLICY r SET create = (false), read = (true), before update = (false), after update = (false), delete = (false), execute = (true)");
    db.command(
        "CREATE SECURITY POLICY rw SET create = (true), read = (true), before update = (true), after update = (true), delete = (true), execute = (true)");

    command("CREATE CLASS Customer extends V ABSTRACT");
    command("CREATE PROPERTY Customer.name String");

    command("CREATE CLASS Customer_t1 extends Customer");
    command("CREATE CLASS Customer_t2 extends Customer");

    command("CREATE CLASS Customer_u1 extends Customer_t1");
    command("CREATE CLASS Customer_u2 extends Customer_t2");

    command("INSERT INTO ORole SET name = 'tenant1', mode = 0");
    db.command("ALTER ROLE tenant1 set policy rw ON database.class.*.*");
    command("UPDATE ORole PUT rules = 'database.class.customer', 2 WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy r ON database.class.Customer");
    command("UPDATE ORole PUT rules = 'database.class.customer_t1', 31 WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy rw ON database.class.Customer_t1");
    command("UPDATE ORole PUT rules = 'database.class.customer_t2', 2 WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy r ON database.class.Custome_t2r");
    command("UPDATE ORole PUT rules = 'database.class.customer_u2', 0 WHERE name = ?", "tenant1");
    command(
        "UPDATE ORole SET inheritedRole = (SELECT FROM ORole WHERE name = 'reader') WHERE name = ?",
        "tenant1");

    command(
        "INSERT INTO OUser set name = 'tenant1', password = 'password', status = 'ACTIVE', roles = (SELECT FROM ORole WHERE name = 'tenant1')");

    command("INSERT INTO ORole SET name = 'tenant2', mode = 0");
    db.command("ALTER ROLE tenant2 set policy rw ON database.class.*.*");
    command("UPDATE ORole PUT rules = 'database.class.customer_t1', 0 WHERE name = ?", "tenant2");
    command("UPDATE ORole PUT rules = 'database.class.customer_t2', 31 WHERE name = ?", "tenant2");
    db.command("ALTER ROLE tenant2 set policy rw ON database.class.Customer_t2");
    command("UPDATE ORole PUT rules = 'database.class.customer', 0 WHERE name = ?", "tenant2");
    command(
        "UPDATE ORole SET inheritedRole = (SELECT FROM ORole WHERE name = 'reader') WHERE name = 'tenant2'");

    command(
        "INSERT INTO OUser set name = 'tenant2', password = 'password', status = 'ACTIVE', roles = (SELECT FROM ORole WHERE name = 'tenant2')");

    command("INSERT INTO Customer_t1 set name='Amy'");
    command("INSERT INTO Customer_t2 set name='Bob'");

    command("INSERT INTO Customer_u1 set name='Fred'");
    command("INSERT INTO Customer_u2 set name='George'");

    db.close();
  }

  private void command(String sql, Object... params) {
    db.command(new OCommandSQL(sql)).execute(params);
  }

  private List<ODocument> query(String sql, Object... params) {
    OSQLSynchQuery<ODocument> query = new OSQLSynchQuery<ODocument>(sql);
    return query.run(params);
  }

  @After
  public void after() {

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
  }

  @Test
  // This tests for a result size of three.  The "Customer_u2" record should not be included.
  public void shouldTestFiltering() {

    db.open("tenant1", "password");

    try {
      List<ODocument> result = query("SELECT FROM Customer");

      assertThat(result).hasSize(3);
    } finally {
      db.close();
    }
  }

  @Test
  // This should return the record in "Customer_t2" but filter out the "Customer_u2" record.
  public void shouldTestCustomer_t2() {

    db.open("tenant1", "password");

    try {
      List<ODocument> result = query("SELECT FROM Customer_t2");

      assertThat(result).hasSize(1);
    } finally {
      db.close();
    }
  }

  @Test(expected = OSecurityAccessException.class)
  // This should throw an OSecurityAccessException when trying to read from the "Customer_u2" class.
  public void shouldTestAccess2() {

    db.open("tenant1", "password");

    try {
      query("SELECT FROM Customer_u2");
    } finally {
      db.close();
    }
  }

  @Test(expected = OSecurityAccessException.class)
  // This should throw an OSecurityAccessException when trying to read from the "Customer" class.
  public void shouldTestCustomer() {

    db.open("tenant2", "password");

    try {
      List<ODocument> result = query("SELECT FROM Customer");
    } finally {
      db.close();
    }
  }

  @Test
  // This tests for a result size of two.  The "Customer_t1" and "Customer_u1" records should not be
  // included.
  public void shouldTestCustomer_t2Tenant2() {

    db.open("tenant2", "password");

    try {
      List<ODocument> result = query("SELECT FROM Customer_t2");

      assertThat(result).hasSize(2);
    } finally {
      db.close();
    }
  }
}
