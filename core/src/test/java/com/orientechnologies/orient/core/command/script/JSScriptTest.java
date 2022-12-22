package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try {
      String script = "db.query('select from OUser')";
      OResultSet resultSet = db.execute("javascript", script);
      Assert.assertEquals(true, resultSet.hasNext());

      List<OResult> results = resultSet.stream().collect(Collectors.toList());
      Assert.assertEquals(1, results.size()); // no default users anymore, 'admin' created

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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try {
      InputStream stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptCountTest.js");
      OResultSet resultSet = db.execute("javascript", OIOUtils.readStreamAsString(stream));
      Assert.assertEquals(true, resultSet.hasNext());

      List<OResult> results = resultSet.stream().collect(Collectors.toList());
      Assert.assertEquals(1, results.size());

      Number value = results.get(0).getProperty("value");
      Assert.assertEquals(1, value.intValue()); // no default users anymore, 'admin' created
    } finally {
      orientDB.drop("test");
    }
    orientDB.close();
  }

  @Test
  public void jsSandboxTestWithJavaType() {
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try {
      final OResultSet result =
          db.execute(
              "javascript", "var File = Java.type(\"java.io.File\");\n  File.pathSeparator;");

      Assert.fail("It should receive a class not found exception");
    } catch (RuntimeException e) {
      Assert.assertEquals(
          OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
              ? ScriptException.class
              : ClassNotFoundException.class,
          e.getCause().getClass());
    } finally {
      orientDB.drop(name.getMethodName());
    }
    orientDB.close();
  }

  // @Test
  // THIS TEST WONT PASS WITH GRAALVM
  public void jsSandboxWithNativeTest() {
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    OScriptManager scriptManager = OrientDBInternal.extract(orientDB).getScriptManager();

    try {
      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.lang.System")));

      OResultSet resultSet =
          db.execute(
              "javascript", "var System = Java.type('java.lang.System'); System.nanoTime();");
      Assert.assertEquals(0, resultSet.stream().count());
    } finally {
      orientDB.drop(name.getMethodName());
      orientDB.close();

      scriptManager.removeAllowedPackages(new HashSet<>(Arrays.asList("java.lang.System")));
    }
  }

  @Test
  public void jsSandboxWithMathTest() {
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final OScriptManager scriptManager = OrientDBInternal.extract(orientDB).getScriptManager();
    try (final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.math.BigDecimal")));

      try (OResultSet resultSet =
          db.execute(
              "javascript",
              "var BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
      scriptManager.removeAllowedPackages(new HashSet<>(Arrays.asList("java.math.BigDecimal")));
      scriptManager.closeAll();

      try {
        db.execute("javascript", "new java.math.BigDecimal(1.0);");
        Assert.fail("It should receive a class not found exception");
      } catch (RuntimeException e) {
        Assert.assertEquals(
            OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
                ? ScriptException.class
                : ClassNotFoundException.class,
            e.getCause().getClass());
      }

      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.math.*")));
      scriptManager.closeAll();

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
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            name.getMethodName(), "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    try (final ODatabaseDocument db =
        orientDB.open(name.getMethodName(), "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      try (OResultSet resultSet =
          db.execute("javascript", "Orient.instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(ScriptException.class, e.getCause().getClass());
      }

      try (OResultSet resultSet =
          db.execute(
              "javascript",
              "com.orientechnologies.orient.core.Orient.instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(
            OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
                ? ScriptException.class
                : ClassNotFoundException.class,
            e.getCause().getClass());
      }

      try (OResultSet resultSet =
          db.execute(
              "javascript",
              "Java.type('com.orientechnologies.orient.core.Orient').instance().getScriptManager().addAllowedPackages([])")) {
        Assert.assertEquals(1, resultSet.stream().count());
      } catch (Exception e) {
        Assert.assertEquals(
            OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
                ? ScriptException.class
                : ClassNotFoundException.class,
            e.getCause().getClass());
      }
    } finally {
      orientDB.drop(name.getMethodName());
      orientDB.close();
    }
  }
}
