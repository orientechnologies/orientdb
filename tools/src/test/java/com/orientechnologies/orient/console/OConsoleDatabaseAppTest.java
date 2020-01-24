package com.orientechnologies.orient.console;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by tglman on 14/03/16.
 */
public class OConsoleDatabaseAppTest {
  @Rule
  public TestName testName = new TestName();

  @Test
  public void testSelectBinaryDoc() throws IOException {
    final StringBuilder builder = new StringBuilder();

    OConsoleDatabaseApp app = new OConsoleDatabaseApp(new String[] {}) {
      @Override
      public void message(String iMessage, Object... iArgs) {
        builder.append(String.format(iMessage, iArgs)).append("\n");
      }
    };
    try {
      app.createDatabase("memory:test", null, null, "memory", null, null);
      ODatabaseDocument db = app.getCurrentDatabase();
      db.addBlobCluster("blobTest");
      ORecord record = db.save(new ORecordBytes("blobContent".getBytes()), "blobTest");
      builder.setLength(0);
      app.select(" from " + record.getIdentity() + " limit -1 ");
      assertTrue(builder.toString().contains("<binary>"));
    } finally {
      app.dropDatabase("memory");
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
    ConsoleTest c = new ConsoleTest(new String[] { builder.toString() });
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      ODatabaseDocument db = console.getCurrentDatabase();
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

    // builder.append("display raw record " + rid);

    // OConsoleDatabaseApp console = new OConsoleDatabaseApp(new String[] { builder.toString() });
    OConsoleDatabaseApp console = new OConsoleDatabaseApp(null);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(out);
    console.setOutput(stream);
    try {
      console.createDatabase(dbUrl, null, null, null, null, null);
      console.createClass("class foo");
      console.insert("into foo set name = 'barbar'");

      byte[] result = out.toByteArray();
      out.close();
      stream.close();
      out = new ByteArrayOutputStream();
      stream = new PrintStream(out);
      console.setOutput(stream);
      String resultString = new String(result);

      String rid = resultString.substring(resultString.indexOf("#"), resultString.indexOf("{") ).trim();

      console.set("maxBinaryDisplay", "10000");
      try {
        console.displayRawRecord(rid);
      } catch (Exception e) {
        OLogManager.instance().error(this, "testDisplayRawRecord - Result from console: " + resultString, e);
        throw e;
      }
      result = out.toByteArray();
      resultString = new String(result);

//      System.out.println("RAW REC: " + resultString);
      Assert.assertTrue(resultString.contains("Raw record content."));
      if ("ORecordSerializerBinary".equals(((ODatabaseDocumentInternal) console.getCurrentDatabase()).getSerializer().toString())) {
//        Assert.assertTrue(resultString.contains("class name: foo"));
        Assert.assertTrue(resultString.contains("property value: barbar"));
      }
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
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().createDatabase("memory:OConsoleDatabaseAppTestDumpRecordDetails", null, null, null, null, null);
      c.console().createClass("class foo");
      c.console().insert("into foo set name = 'barbar'");
      c.console().select("from foo limit -1");
      c.resetOutput();

      c.console().set("maxBinaryDisplay", "10000");
      c.console().displayRecord("0");

      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("@class:foo"));
      Assert.assertTrue(resultString.contains("barbar"));
    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }

  }

  @Test
  public void testHelp() {
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().help(null);
      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("connect"));
      Assert.assertTrue(resultString.contains("alter class"));
      Assert.assertTrue(resultString.contains("create class"));
      Assert.assertTrue(resultString.contains("select"));
      Assert.assertTrue(resultString.contains("update"));
      Assert.assertTrue(resultString.contains("delete"));
      Assert.assertTrue(resultString.contains("create vertex"));
      Assert.assertTrue(resultString.contains("create edge"));
      Assert.assertTrue(resultString.contains("help"));
      Assert.assertTrue(resultString.contains("exit"));

    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testHelpCommand() {
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().help("select");
      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("COMMAND: select"));

    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testDeclareIntent() {
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().createDatabase("memory:OConsoleDatabaseAppTestDeclareIntent", null, null, null, null, null);
      c.resetOutput();
      try {
        c.console().declareIntent("foobar");
        Assert.fail();
      } catch (Exception e) {

      }

      c.resetOutput();
      c.console().declareIntent("massiveinsert");
      c.console().declareIntent("massiveread");
      c.console().declareIntent("null");

      String resultString = c.getConsoleOutput();

      Assert.assertTrue(resultString.contains("Intent 'massiveinsert' set successfully"));
      Assert.assertTrue(resultString.contains("Intent 'massiveread' set successfully"));
      Assert.assertTrue(resultString.contains("Intent 'null' set successfully"));

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testSimple() {
    String dbUrl = "memory:" + testName.getMethodName();
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("profile storage on;\n");
    builder.append("create class foo;\n");
    builder.append("config;\n");
    builder.append("list classes;\n");
    builder.append("list properties;\n");
    builder.append("list clusters;\n");
    builder.append("list indexes;\n");
    builder.append("info class OUser;\n");
    builder.append("info property OUser.name;\n");

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

    builder.append("profile storage off;\n");

    builder.append("repair database -v;\n");
    ConsoleTest c = new ConsoleTest(new String[] { builder.toString() });
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      ODatabaseDocument db = console.getCurrentDatabase();
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from foo where name = 'foo'"));
      Assert.assertEquals(1, result.size());
      ODocument doc = result.get(0);
      Assert.assertEquals("bar", doc.field("surname"));

      result = db.query(new OSQLSynchQuery<ODocument>("select from bar"));
      Assert.assertEquals(0, result.size());

    } finally {
      console.close();
    }

  }

  @Test
  @Ignore
  public void testMultiLine() {
    String dbUrl = "memory:" + testName.getMethodName();
    StringBuilder builder = new StringBuilder();
    builder.append("create database " + dbUrl + ";\n");
    builder.append("profile storage on;\n");
    builder.append("create class foo;\n");
    builder.append("config;\n");
    builder.append("list classes;\n");
    builder.append("list properties;\n");
    builder.append("list clusters;\n");
    builder.append("list indexes;\n");
    builder.append("info class OUser;\n");
    builder.append("info property OUser.name;\n");

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

//    builder.append("create edge from (select from V where name = 'foo') to (select from V where name = 'bar');\n");

    builder.append("create edge from \n" + "(select from V where name = 'foo') \n" + "to (select from V where name = 'bar');\n");

    ConsoleTest c = new ConsoleTest(new String[] { builder.toString() });
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      ODatabaseDocument db = console.getCurrentDatabase();
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from foo where name = 'foo'"));
      Assert.assertEquals(1, result.size());
      ODocument doc = result.get(0);
      Assert.assertEquals("bar", doc.field("surname"));

      result = db.query(new OSQLSynchQuery<ODocument>("select from bar"));
      Assert.assertEquals(0, result.size());

    } finally {
      console.close();
    }

  }

  class ConsoleTest {
    OConsoleDatabaseApp   console;
    ByteArrayOutputStream out;
    PrintStream           stream;

    ConsoleTest() {
      console = new OConsoleDatabaseApp(null) {
        @Override
        protected void onException(Throwable e) {
          super.onException(e);
          fail(e.getMessage());
        }
      };
      resetOutput();
    }

    ConsoleTest(String[] args) {
      console = new OConsoleDatabaseApp(args) {
        @Override
        protected void onException(Throwable e) {
          super.onException(e);
          fail(e.getMessage());
        }
      };
      resetOutput();
    }

    public OConsoleDatabaseApp console() {
      return console;
    }

    public String getConsoleOutput() {
      byte[] result = out.toByteArray();
      return new String(result);
    }

    void resetOutput() {
      if (out != null) {
        try {
          stream.close();
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      out = new ByteArrayOutputStream();
      stream = new PrintStream(out);
      console.setOutput(stream);
    }

    void shutdown() {
      if (out != null) {
        try {
          stream.close();
          out.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      console.close();
    }
  }

}
