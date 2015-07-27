/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
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
 *
 */

package com.orientechnologies.orient.graph.console;

import com.orientechnologies.orient.console.OConsoleDatabaseApp;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;

/**
 * @author Luigi Dell'Aquila
 */
public class OConsoleDatabaseAppTest {

  @Test
  public void testSimple() {
    String dbUrl = "memory:OConsoleDatabaseAppTest";
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("create class foo;\n");
    builder.append("config;\n");
    builder.append("list classes;\n");
    builder.append("list properties;\n");
    builder.append("list clusters;\n");
    builder.append("list indexes;\n");
    builder.append("info class OUser;\n");

    builder.append("begin;\n");
    builder.append("insert into foo set name = 'foo';\n");
    builder.append("insert into foo set name = 'bla';\n");
    builder.append("update foo set surname = 'bar' where name = 'foo';\n");
    builder.append("commit;\n");
    builder.append("select from foo;\n");

    builder.append("create class bar;\n");
    builder.append("create property bar.name STRING;\n");
    builder.append("create index bar_name on bar (name) NOTUNIQUE;\n");

    builder.append("insert into bar set name = 'foo';\n");
    builder.append("delete from bar;\n");
    builder.append("begin;\n");
    builder.append("insert into bar set name = 'foo';\n");
    builder.append("rollback;\n");

    builder.append("create vertex V set name = 'foo';\n");
    builder.append("create vertex V set name = 'bar';\n");

    builder.append("traverse out() from V;\n");

    builder.append("create edge from (select from V where name = 'foo') to (select from V where name = 'bar');\n");

    builder.append("traverse out() from V;\n");

    OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { builder.toString() });

    try {
      console.run();

      ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
      db.open("admin", "admin");
      try {
        List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from foo where name = 'foo'"));
        Assert.assertEquals(1, result.size());
        ODocument doc = result.get(0);
        Assert.assertEquals("bar", doc.field("surname"));

        result = db.query(new OSQLSynchQuery<ODocument>("select from bar"));
        Assert.assertEquals(0, result.size());
      } finally {
        db.close();
      }
      OrientGraph graph = new OrientGraph(dbUrl);
      try {
        Iterable<Vertex> result = graph.command(
            new OSQLSynchQuery<Vertex>("select expand(out()) from (select from V where name = 'foo')")).execute();
        Iterator<Vertex> iterator = result.iterator();
        Assert.assertTrue(iterator.hasNext());
        Vertex next = iterator.next();
        Assert.assertEquals("bar", next.getProperty("name"));
        Assert.assertFalse(iterator.hasNext());
      } finally {
        graph.shutdown();
      }
    } finally {
      console.close();
    }

  }

  @Test
  public void testWrongCommand() {
    String dbUrl = "memory:OConsoleDatabaseAppTest2";
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("create class foo;\n");
    builder.append("insert into foo set name = 'foo';\n");
    builder.append("insert into foo set name = 'bla';\n");
    builder.append("blabla;\n");// <- wrong command, this should break the console
    builder.append("update foo set surname = 'bar' where name = 'foo';\n");
    OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { builder.toString() });

    try {
      console.run();

      ODatabaseDocumentTx db = new ODatabaseDocumentTx(dbUrl);
      db.open("admin", "admin");
      try {
        List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from foo where name = 'foo'"));
        Assert.assertEquals(1, result.size());
        ODocument doc = result.get(0);
        Assert.assertNull(doc.field("surname"));
      } finally {
        db.close();
      }
    } finally {
      console.close();
    }

  }

  @Test
  public void testDisplayRawRecord() {
    String dbUrl = "memory:OConsoleDatabaseAppTestDisplayRawRecord";
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("create class foo;\n");
    builder.append("insert into foo set name = 'foo';\n");

    // builder.append("display raw record " + rid);

    // OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { builder.toString() });
    OConsoleDatabaseApp console = new OConsoleDatabaseApp(null);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    console.setOutput(stream);
    try {
      console.createDatabase(dbUrl, null, null, null, null);
      console.createClass("class foo");
      console.insert("into foo set name = 'barbar'");

      byte[] result = out.toByteArray();
      out.close();
      stream.close();
      out = new ByteArrayOutputStream();
      stream = new PrintStream(out);
      console.setOutput(stream);
      String resultString = new String(result);

      String rid = resultString.substring(resultString.indexOf("#"), resultString.indexOf("#") + 5).trim();

      console.set("maxBinaryDisplay", "10000");
      console.displayRawRecord(rid);
      result = out.toByteArray();
      resultString = new String(result);
      Assert.assertTrue(resultString.contains("class name: foo"));
      Assert.assertTrue(resultString.contains("property value: barbar"));
    } catch (IOException e) {
      Assert.fail();
    } finally {
      try {
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        stream.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      console.close();
    }

  }

  @Test
  public void testDumpRecordDetails() {
    String dbUrl = "memory:OConsoleDatabaseAppTestDumpRecordDetails";
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("create class foo;\n");
    builder.append("insert into foo set name = 'foo';\n");

    // builder.append("display raw record " + rid);

    // OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { builder.toString() });
    OConsoleDatabaseApp console = new OConsoleDatabaseApp(null);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    console.setOutput(stream);
    try {
      console.createDatabase(dbUrl, null, null, null, null);
      console.createClass("class foo");
      console.insert("into foo set name = 'barbar'");
      console.select("from foo limit -1");

      byte[] result = out.toByteArray();
      out.close();
      stream.close();
      out = new ByteArrayOutputStream();
      stream = new PrintStream(out);
      console.setOutput(stream);
//      String resultString = new String(result);

//      String rid = resultString.substring(resultString.indexOf("#"), resultString.indexOf("#") + 5).trim();

      console.set("maxBinaryDisplay", "10000");
      console.displayRecord("0");
      result = out.toByteArray();
      String resultString = new String(result);
      Assert.assertTrue(resultString.contains("@class: foo"));
      Assert.assertTrue(resultString.contains("barbar"));
    } catch (Exception e) {
      Assert.fail();
    } finally {
      try {
        out.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      try {
        stream.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      console.close();
    }

  }
}
