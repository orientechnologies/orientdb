package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 27/01/17.
 */
public class JSScriptTest {

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

      results.stream().map(r -> r.getElement().get()).forEach(oElement -> {
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
      values.stream().map(r -> r.getElement().get()).forEach(oElement -> {
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
}
