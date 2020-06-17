package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/24/14
 */
@Test
public class SQLCreateVertexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLCreateVertexTest(@Optional String url) {
    super(url);
  }

  public void testCreateVertexByContent() {
    System.out.println(System.getProperty("file.encoding"));
    System.out.println(Charset.defaultCharset());
    OrientGraph graph = new OrientGraph(database, false);
    graph.shutdown();
    database.open("admin", "admin");

    OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("CreateVertexByContent")) {
      OClass vClass = schema.createClass("CreateVertexByContent", schema.getClass("V"));
      vClass.createProperty("message", OType.STRING);
    }

    database
        .command(
            new OCommandSQL("create vertex CreateVertexByContent content { \"message\": \"(:\"}"))
        .execute();
    database
        .command(
            new OCommandSQL(
                "create vertex CreateVertexByContent content { \"message\": \"\\\"‎ה, כן?...‎\\\"\"}"))
        .execute();

    List<ODocument> result =
        database.query(new OSQLSynchQuery<ODocument>("select from CreateVertexByContent"));
    Assert.assertEquals(result.size(), 2);

    List<String> messages = new ArrayList<String>();
    messages.add("\"‎ה, כן?...‎\"");
    messages.add("(:");

    List<String> resultMessages = new ArrayList<String>();

    for (ODocument document : result) {
      resultMessages.add(document.<String>field("message"));
    }

    //    issue #1787, works fine locally, not on CI
    Assert.assertEqualsNoOrder(
        messages.toArray(),
        resultMessages.toArray(),
        "arrays are different: " + toString(messages) + " - " + toString(resultMessages));
  }

  private String toString(List<String> resultMessages) {
    StringBuilder result = new StringBuilder();
    result.append("[");
    boolean first = true;
    for (String msg : resultMessages) {
      if (!first) {
        result.append(", ");
      }
      result.append("\"");
      result.append(msg);
      result.append("\"");
      first = false;
    }
    result.append("]");
    return result.toString();
  }

  public void testCreateVertexBooleanProp() {
    OrientGraph graph = new OrientGraph(database, false);
    graph.shutdown();
    database.open("admin", "admin");

    database.command(new OCommandSQL("create vertex set script = true")).execute();
    database.command(new OCommandSQL("create vertex")).execute();
    database.command(new OCommandSQL("create vertex V")).execute();

    // TODO complete this!
    // database.command(new OCommandSQL("create vertex set")).execute();
    // database.command(new OCommandSQL("create vertex set set set = 1")).execute();

  }

  public void testIsClassName() {
    OrientGraph graph = new OrientGraph(database, false);
    graph.shutdown();
    database.open("admin", "admin");
    graph.createVertexType("Like").createProperty("anything", OType.STRING);
    graph.createVertexType("Is").createProperty("anything", OType.STRING);
  }
}
