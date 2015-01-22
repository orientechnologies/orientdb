package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 3/24/14
 */
@Test
public class SQLCreateVertexTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public SQLCreateVertexTest(@Optional String url) {
    super(url);
  }

  public void testCreateVertexByContent() {
		OrientGraph graph = new OrientGraph(database, false);
		graph.shutdown();
		database.open("admin", "admin");

    OSchema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("CreateVertexByContent")) {
      OClass vClass = schema.createClass("CreateVertexByContent", schema.getClass("V"));
      vClass.createProperty("message", OType.STRING);
    }

    database.command(new OCommandSQL("create vertex CreateVertexByContent content { \"message\": \"(:\"}")).execute();
    database.command(new OCommandSQL("create vertex CreateVertexByContent content { \"message\": \"\\\"‎ה, כן?...‎\\\"\"}")).execute();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from CreateVertexByContent"));
    Assert.assertEquals(result.size(), 2);

		List<String> messages = new ArrayList<String>();
		messages.add("\"‎ה, כן?...‎\"");
		messages.add("(:");

		for (ODocument document : result) {
			Assert.assertTrue(messages.remove(document.<String>field("message")));
		}

		Assert.assertEquals(messages.size(), 0);
  }
}
