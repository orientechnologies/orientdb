package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OWhileBlockExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testPlain() {

    String className = "testPlain";

    db.createClass(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  LET $i = $i + 1;";
    script += "}";
    script += "SELECT FROM " + className;

    OResultSet results = db.execute("sql", script);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      OResult item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assert.assertEquals(3, tot);
    Assert.assertEquals(3, sum);
    results.close();
  }

  @Test
  public void testReturn() {
    String className = "testReturn";

    db.createClass(className);

    String script = "";
    script += "LET $i = 0;";
    script += "WHILE ($i < 3){\n";
    script += "  insert into " + className + " set value = $i;\n";
    script += "  IF ($i = 1) {";
    script += "    RETURN;";
    script += "  }";
    script += "  LET $i = $i + 1;";
    script += "}";

    OResultSet results = db.execute("sql", script);
    results.close();
    results = db.query("SELECT FROM " + className);

    int tot = 0;
    int sum = 0;
    while (results.hasNext()) {
      OResult item = results.next();
      sum += (Integer) item.getProperty("value");
      tot++;
    }
    Assert.assertEquals(2, tot);
    Assert.assertEquals(1, sum);
    results.close();
  }
}
