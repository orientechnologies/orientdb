package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
import org.junit.Test;

/** Created by Enrico Risa on 27/01/17. */
public abstract class JSScriptTest extends BaseMemoryDatabase {
  public Class<?> expectedException;

  public JSScriptTest(Class<?> expectedException) {
    this.expectedException = expectedException;
  }

  @Test
  public void jsSimpleTest() {
    OResultSet resultSet = db.execute("javascript", "'foo'");
    Assert.assertEquals(true, resultSet.hasNext());
    OResult result = resultSet.next();
    String ret = result.getProperty("value");
    Assert.assertEquals("foo", ret);
  }

  @Test
  public void jsQueryTest() {
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
  }

  @Test
  public void jsScriptTest() throws IOException {
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
  }

  @Test
  public void jsScriptCountTest() throws IOException {
    InputStream stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptCountTest.js");
    OResultSet resultSet = db.execute("javascript", OIOUtils.readStreamAsString(stream));
    Assert.assertEquals(true, resultSet.hasNext());

    List<OResult> results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(1, results.size());

    Number value = results.get(0).getProperty("value");
    Assert.assertEquals(1, value.intValue()); // no default users anymore, 'admin' created
  }

  @Test
  public void jsSandboxTestWithJavaType() {
    try {
      final OResultSet result =
          db.execute(
              "javascript", "var File = Java.type(\"java.io.File\");\n  File.pathSeparator;");

      Assert.fail("It should receive a class not found exception");
    } catch (RuntimeException e) {
      Assert.assertEquals(expectedException, e.getCause().getClass());
    }
  }

  // @Test
  // THIS TEST WONT PASS WITH GRAALVM
  public void jsSandboxWithNativeTest() {
    OScriptManager scriptManager = OrientDBInternal.extract(context).getScriptManager();

    try {
      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.lang.System")));

      OResultSet resultSet =
          db.execute(
              "javascript", "var System = Java.type('java.lang.System'); System.nanoTime();");
      Assert.assertEquals(0, resultSet.stream().count());
    } finally {
      scriptManager.removeAllowedPackages(new HashSet<>(Arrays.asList("java.lang.System")));
    }
  }

  @Test
  public void jsSandboxWithMathTest() {
    OResultSet resultSet = db.execute("javascript", "Math.random()");
    Assert.assertEquals(1, resultSet.stream().count());
  }

  @Test
  public void jsSandboxWithDB() {
    OResultSet resultSet =
        db.execute(
            "javascript",
            "var elem = db.query(\"select from OUser\").stream().findFirst().get();"
                + " elem.getProperty(\"name\")");
    Assert.assertEquals(1, resultSet.stream().count());
  }

  @Test
  public void jsSandboxWithBigDecimal() {
    OScriptManager scriptManager = OrientDBInternal.extract(context).getScriptManager();

    try {
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
        Assert.assertEquals(expectedException, e.getCause().getClass());
      }

      scriptManager.addAllowedPackages(new HashSet<>(Arrays.asList("java.math.*")));
      scriptManager.closeAll();

      try (OResultSet resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

    } finally {
      scriptManager.removeAllowedPackages(
          new HashSet<>(Arrays.asList("java.math.BigDecimal", "java.math.*")));
    }
  }

  @Test
  public void jsSandboxWithOrient() {
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
      Assert.assertEquals(expectedException, e.getCause().getClass());
    }

    try (OResultSet resultSet =
        db.execute(
            "javascript",
            "Java.type('com.orientechnologies.orient.core.Orient').instance().getScriptManager().addAllowedPackages([])")) {
      Assert.assertEquals(1, resultSet.stream().count());
    } catch (Exception e) {
      Assert.assertEquals(expectedException, e.getCause().getClass());
    }
  }

  @Test
  public void functionSqlWithInnerFunctionJs() {

    ODocument doc1 = new ODocument("Test");
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1.reset();
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);

    OFunction function = new OFunction();
    function.setName("test");
    function.setCode("select name from Test where name = :name and hello(:name) = 'Hello Enrico'");
    function.setParameters(List.of("name"));
    function.save(db);

    OFunction function1 = new OFunction();
    function1.setName("hello");
    function1.setLanguage("javascript");
    function1.setCode("return 'Hello ' + name");
    function1.setParameters(List.of("name"));
    function1.save(db);
    Object result = function.executeInContext(new OBasicCommandContext(), "Enrico");

    Assert.assertEquals(((OResultSet) result).stream().count(), 1);
  }

  @Test
  public void testCreateFunction() {
    db.command(
            "CREATE FUNCTION testCreateFunction \"return 'hello '+name;\" PARAMETERS [name]"
                + " IDEMPOTENT true LANGUAGE Javascript")
        .close();
    OResultSet result = db.command("select testCreateFunction('world') as name");
    Assert.assertEquals(result.next().getProperty("name"), "hello world");
    Assert.assertFalse(result.hasNext());
  }

  @Test
  public void testPlainCreateFunction() {
    String name = "testPlain";
    OResultSet result =
        db.command(
            "CREATE FUNCTION " + name + " \"return a + b;\" PARAMETERS [a,b] language javascript");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals(name, next.getProperty("functionName"));
    result.close();

    result = db.query("select " + name + "('foo', 'bar') as sum");
    Assert.assertTrue(result.hasNext());
    next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    Assert.assertEquals("foobar", next.getProperty("sum"));
    result.close();
  }

  public void jsFunctionArray() throws IOException {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("testFunction");
    func.setLanguage("javascript");
    func.setCode(
        "     \n"
            + "\n"
            + "// Convert to JS native array\n"
            + "var jsList = [];\n"
            + "for (var i = 0; i < 10; i++) {\n"
            + "    jsList.push(i);\n"
            + "}\n"
            + "\n"
            + "return jsList;");
    func.execute();
  }
}
