package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.script.ScriptException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/** Created by Enrico Risa on 27/01/17. */
public class JSScriptTest {

  @Rule public TestName name = new TestName();

  @Test
  public void jsSimpleTest() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open("test", "admin", "admin");
    try {
      OResultSet resultSet = db.execute("javascript", "'foo'");
      Assert.assertEquals(true, resultSet.hasNext());
      OResult result = resultSet.next();
      String ret = result.getProperty("value");
      Assert.assertEquals("foo", ret);
    } finally {
      orientDB.drop("test");
    }
    orientDB.close();
  }

  @Test
  public void jsQueryTest() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open("test", "admin", "admin");
    try {

      String script = "db.query('select from OUser')";
      OResultSet resultSet = db.execute("javascript", script);
      Assert.assertEquals(true, resultSet.hasNext());

      List<OResult> results = resultSet.stream().collect(Collectors.toList());
      Assert.assertEquals(3, results.size());

      results.stream()
          .map(r -> r.getElement().get())
          .forEach(
              oElement -> {
                Assert.assertEquals("OUser", oElement.getSchemaType().get().getName());
              });

    } finally {
      orientDB.drop("test");
    }
    orientDB.close();
  }

  @Test
  public void jsScriptTest() throws IOException {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open("test", "admin", "admin");
    try {

      InputStream stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptTest.js");
      OResultSet resultSet = db.execute("javascript", OIOUtils.readStreamAsString(stream));
      Assert.assertEquals(true, resultSet.hasNext());

      List<OResult> results = resultSet.stream().collect(Collectors.toList());
      Assert.assertEquals(1, results.size());

      Object value = results.get(0).getProperty("value");
      Collection<OResult> values = (Collection<OResult>) value;
      values.stream()
          .map(r -> r.getElement().get())
          .forEach(
              oElement -> {
                Assert.assertEquals("OUser", oElement.getSchemaType().get().getName());
              });

    } finally {
      orientDB.drop("test");
    }
    orientDB.close();
  }

  @Test
  public void jsScriptCountTest() throws IOException {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open("test", "admin", "admin");
    try {

      InputStream stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptCountTest.js");
      OResultSet resultSet = db.execute("javascript", OIOUtils.readStreamAsString(stream));
      Assert.assertEquals(true, resultSet.hasNext());

      List<OResult> results = resultSet.stream().collect(Collectors.toList());
      Assert.assertEquals(1, results.size());

      Number value = results.get(0).getProperty("value");
      Assert.assertEquals(3, value.intValue());
    } finally {
      orientDB.drop("test");
    }
    orientDB.close();
  }

  @Test
  public void jsSandboxTestWithJavaType() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin");
    try {

      db.execute("javascript", "var File = Java.type(\"java.io.File\");\n  File.pathSeparator;");

      Assert.fail("It should receive a class not found exception");
    } catch (RuntimeException e) {
      Assert.assertEquals(e.getCause().getClass(), ClassNotFoundException.class);
    } finally {
      orientDB.drop(name.getMethodName());
    }
    orientDB.close();
  }

  @Test
  public void jsSandboxWithNativeTest() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin");
    try {

      OResultSet resultSet =
          db.execute("javascript", "var File = java.io.File; File.pathSeparator;");
      Assert.assertEquals(0, resultSet.stream().count());
    } finally {
      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }

  @Test
  public void jsSandboxWithMathTest() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin");
    try {

      OResultSet resultSet = db.execute("javascript", "Math.random()");
      Assert.assertEquals(1, resultSet.stream().count());
    } finally {
      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }

  @Test
  public void jsSandboxWithDB() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin");
    try {

      OResultSet resultSet =
          db.execute(
              "javascript",
              "var elem = db.query(\"select from OUser\").stream().findFirst().get(); elem.getProperty(\"name\")");
      Assert.assertEquals(1, resultSet.stream().count());
    } finally {
      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }

  @Test
  public void jsSandboxWithBigDecimal() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);

    OScriptManager scriptManager = OrientDBInternal.extract(orientDB).getScriptManager();
    try (ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin")) {

      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.math.BigDecimal")));

      try (OResultSet resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

      scriptManager.removeAllowedPackages(new HashSet<>(Arrays.asList("java.math.BigDecimal")));

      try {
        db.execute("javascript", "new java.math.BigDecimal(1.0);");
        Assert.fail("It should receive a class not found exception");
      } catch (RuntimeException e) {
        Assert.assertEquals(e.getCause().getClass(), ClassNotFoundException.class);
      }

      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.math.*")));

      try (OResultSet resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

    } finally {
      scriptManager.removeAllowedPackages(
          new HashSet<>(Arrays.asList("java.math.BigDecimal", "java.math.*")));
      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }

  @Test
  public void jsSandboxWithOrient() {

    OrientDB orientDB = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDB.create(name.getMethodName(), ODatabaseType.MEMORY);
    try (ODatabaseDocument db = orientDB.open(name.getMethodName(), "admin", "admin")) {

      try (OResultSet resultSet =
          db.execute("javascript", "Orient.instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(e.getCause().getClass(), ScriptException.class);
      }

      try (OResultSet resultSet =
          db.execute(
              "javascript",
              "com.orientechnologies.orient.core.Orient.instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(e.getCause().getClass(), ClassNotFoundException.class);
      }

      try (OResultSet resultSet =
          db.execute(
              "javascript",
              "Java.type('com.orientechnologies.orient.core.Orient').instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(e.getCause().getClass(), ClassNotFoundException.class);
      }

    } finally {

      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }
}
