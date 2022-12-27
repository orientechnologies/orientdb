package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 10/02/17. */
public class OSqlScriptExecutorTest {
  @Test
  public void testPlain() {
    final OrientDB factory =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final String dbName = getClass().getSimpleName() + "test";
    OCreateDatabaseUtil.createDatabase(dbName, factory, OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        factory.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    String script = "insert into V set name ='a';";
    script += "insert into V set name ='b';";
    script += "insert into V set name ='c';";
    script += "insert into V set name ='d';";
    script += "select from v;";

    OResultSet result = db.execute("sql", script);
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));
    Assert.assertTrue(list.contains("b"));
    Assert.assertTrue(list.contains("c"));
    Assert.assertTrue(list.contains("d"));
    Assert.assertEquals(4, list.size());

    db.close();
    factory.drop(dbName);
    factory.close();
  }

  @Test
  public void testWithPositionalParams() {
    final OrientDB factory =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final String dbName = getClass().getSimpleName() + "test";
    OCreateDatabaseUtil.createDatabase(dbName, factory, OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        factory.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    String script = "insert into V set name ='a';";
    script += "insert into V set name ='b';";
    script += "insert into V set name ='c';";
    script += "insert into V set name ='d';";
    script += "select from v where name = ?;";

    OResultSet result = db.execute("sql", script, "a");
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));

    Assert.assertEquals(1, list.size());

    db.close();
    factory.drop(dbName);
    factory.close();
  }

  @Test
  public void testWithNamedParams() {
    final OrientDB factory =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final String dbName = getClass().getSimpleName() + "test";
    OCreateDatabaseUtil.createDatabase(dbName, factory, OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        factory.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    String script = "insert into V set name ='a';";
    script += "insert into V set name ='b';";
    script += "insert into V set name ='c';";
    script += "insert into V set name ='d';";
    script += "select from v where name = :name;";

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("name", "a");
    OResultSet result = db.execute("sql", script, params);
    List<Object> list =
        result.stream().map(x -> x.getProperty("name")).collect(Collectors.toList());
    result.close();

    Assert.assertTrue(list.contains("a"));

    Assert.assertEquals(1, list.size());

    db.close();
    factory.drop(dbName);
    factory.close();
  }

  @Test
  public void testMultipleCreateEdgeOnTheSameLet() {
    // issue #7635
    final OrientDB factory =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final String dbName = getClass().getSimpleName() + "test";
    OCreateDatabaseUtil.createDatabase(dbName, factory, OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        factory.open(dbName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    String script = "begin;";
    script += "let $v1 = create vertex v set name = 'Foo';";
    script += "let $v2 = create vertex v set name = 'Bar';";
    script += "create edge from $v1 to $v2;";
    script += "let $v3 = create vertex v set name = 'Baz';";
    script += "create edge from $v1 to $v3;";
    script += "commit;";

    OResultSet result = db.execute("sql", script);
    result.close();

    result = db.query("SELECT expand(out()) FROM V WHERE name ='Foo'");
    Assert.assertEquals(2, result.stream().count());
    result.close();
    db.close();
    factory.drop(dbName);
    factory.close();
  }
}
