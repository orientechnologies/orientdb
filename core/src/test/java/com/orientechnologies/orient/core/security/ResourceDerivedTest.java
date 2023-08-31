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

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author SDIPro */
public class ResourceDerivedTest {

  private OrientDB orientDB;

  @Before
  public void before() {
    orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession db = orientDB.open("test", "admin", "admin");

    db.command(
        "CREATE SECURITY POLICY r SET create = (false), read = (true), before update = (false), after update = (false), delete = (false), execute = (true)");
    db.command(
        "CREATE SECURITY POLICY rw SET create = (true), read = (true), before update = (true), after update = (true), delete = (true), execute = (true)");

    db.command("CREATE CLASS Customer extends V ABSTRACT");
    db.command("CREATE PROPERTY Customer.name String");

    db.command("CREATE CLASS Customer_t1 extends Customer");
    db.command("CREATE CLASS Customer_t2 extends Customer");

    db.command("CREATE CLASS Customer_u1 extends Customer_t1");
    db.command("CREATE CLASS Customer_u2 extends Customer_t2");

    db.command("INSERT INTO ORole SET name = 'tenant1', mode = 0");
    db.command("ALTER ROLE tenant1 set policy rw ON database.class.*.*");
    db.command("UPDATE ORole SET rules = {'database.class.customer': 2} WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy r ON database.class.Customer");
    db.command(
        "UPDATE ORole SET rules = {'database.class.customer_t1': 31} WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy rw ON database.class.Customer_t1");
    db.command(
        "UPDATE ORole SET rules = {'database.class.customer_t2': 2} WHERE name = ?", "tenant1");
    db.command("ALTER ROLE tenant1 set policy r ON database.class.Custome_t2r");
    db.command(
        "UPDATE ORole SET rules = {'database.class.customer_u2': 0} WHERE name = ?", "tenant1");
    db.command(
        "UPDATE ORole SET inheritedRole = (SELECT FROM ORole WHERE name = 'reader') WHERE name = ?",
        "tenant1");

    db.command(
        "INSERT INTO OUser set name = 'tenant1', password = 'password', status = 'ACTIVE', roles = (SELECT FROM ORole WHERE name = 'tenant1')");

    db.command("INSERT INTO ORole SET name = 'tenant2', mode = 0");
    db.command("ALTER ROLE tenant2 set policy rw ON database.class.*.*");
    db.command(
        "UPDATE ORole SET rules = {'database.class.customer_t1': 0} WHERE name = ?", "tenant2");
    db.command(
        "UPDATE ORole SET rules = {'database.class.customer_t2': 31} WHERE name = ?", "tenant2");
    db.command("ALTER ROLE tenant2 set policy rw ON database.class.Customer_t2");
    db.command("UPDATE ORole SET rules = {'database.class.customer': 0} WHERE name = ?", "tenant2");
    db.command(
        "UPDATE ORole SET inheritedRole = (SELECT FROM ORole WHERE name = 'reader') WHERE name = 'tenant2'");

    db.command(
        "INSERT INTO OUser set name = 'tenant2', password = 'password', status = 'ACTIVE', roles = (SELECT FROM ORole WHERE name = 'tenant2')");

    db.command("INSERT INTO Customer_t1 set name='Amy'");
    db.command("INSERT INTO Customer_t2 set name='Bob'");

    db.command("INSERT INTO Customer_u1 set name='Fred'");
    db.command("INSERT INTO Customer_u2 set name='George'");

    db.close();
  }

  private OResultSet query(ODatabaseSession db, String sql, Object... params) {
    return db.query(sql, params);
  }

  @After
  public void after() {
    orientDB.close();
  }

  @Test
  // This tests for a result size of three.  The "Customer_u2" record should not be included.
  public void shouldTestFiltering() {

    ODatabaseSession db = orientDB.open("test", "tenant1", "password");

    try {
      OResultSet result = query(db, "SELECT FROM Customer");

      assertThat(result).hasSize(3);
    } finally {
      db.close();
    }
  }

  @Test
  // This should return the record in "Customer_t2" but filter out the "Customer_u2" record.
  public void shouldTestCustomer_t2() {

    ODatabaseSession db = orientDB.open("test", "tenant1", "password");

    try {
      OResultSet result = query(db, "SELECT FROM Customer_t2");

      assertThat(result).hasSize(1);
    } finally {
      db.close();
    }
  }

  public void shouldTestAccess2() {

    ODatabaseSession db = orientDB.open("test", "tenant1", "password");

    try {
      OResultSet result = query(db, "SELECT FROM Customer_u2");
      assertThat(result).hasSize(0);
    } finally {
      db.close();
    }
  }

  public void shouldTestCustomer() {

    ODatabaseSession db = orientDB.open("test", "tenant2", "password");

    try {
      OResultSet result = query(db, "SELECT FROM Customer");
      assertThat(result).hasSize(0);
    } finally {
      db.close();
    }
  }

  @Test
  // This tests for a result size of two.  The "Customer_t1" and "Customer_u1" records should not be
  // included.
  public void shouldTestCustomer_t2Tenant2() {

    ODatabaseSession db = orientDB.open("test", "tenant2", "password");

    try {
      OResultSet result = query(db, "SELECT FROM Customer_t2");

      assertThat(result).hasSize(2);
    } finally {
      db.close();
    }
  }
}
