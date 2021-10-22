package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test HTTP "Graph" .
 *
 * @author Enrico Risa (e.risa--at-orientdb.com)
 */
public class HttpGraphTest extends BaseHttpDatabaseTest {

  @Test
  public void updateWithEdges() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("create class Foo extends V", CONTENT.TEXT)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("create class FooEdge extends E", CONTENT.TEXT)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    String script = "begin;";
    script += "let $v1 = create vertex Foo set name = 'foo1';";
    script += "let $v2 = create vertex Foo set name = 'foo2';";
    script += "create edge FooEdge from $v1 to $v2;";
    script += "commit;";
    script += "return $v1;";

    final String scriptPayload =
        "{ \"operations\" : [{ \"type\" : \"script\", \"language\" : \"SQL\",  \"script\" : \"%s\"}]}";

    HttpResponse response =
        post("batch/" + getDatabaseName() + "/sql/")
            .payload(String.format(scriptPayload, script), CONTENT.JSON)
            .getResponse();
    Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);

    final ODocument result = new ODocument().fromJSON(response.getEntity().getContent());

    final List<ODocument> res = result.field("result");
    Assert.assertEquals(res.size(), 1);
    ODocument created = res.get(0);
    Assert.assertEquals(created.field("name"), "foo1");
    Assert.assertEquals(created.getVersion(), 1);

    ORidBag coll = created.field("out_FooEdge");
    Assert.assertEquals(coll.size(), 1);
    created.field("name", "fooUpdated");

    response =
        put("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1))
            .payload(created.toJSON(), CONTENT.JSON)
            .exec()
            .getResponse();
    Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);

    final ODocument updated = new ODocument().fromJSON(response.getEntity().getContent());
    Assert.assertEquals(updated.field("name"), "fooUpdated");
    Assert.assertEquals(updated.getVersion(), 2);
    coll = updated.field("out_FooEdge");
    Assert.assertEquals(coll.size(), 1);
  }

  @Test
  public void getGraphResult() throws IOException {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("create class Foo extends V", CONTENT.TEXT)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload("create class FooEdge extends E", CONTENT.TEXT)
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    String script = "begin;";
    script += "let $v1 = create vertex Foo set name = 'foo1';";
    script += "let $v2 = create vertex Foo set name = 'foo2';";
    script += "create edge FooEdge from $v1 to $v2;";
    script += "commit;";
    script += "return $v1;";

    final String scriptPayload =
        "{ \"operations\" : [{ \"type\" : \"script\", \"language\" : \"SQL\",  \"script\" : \"%s\"}]}";

    HttpResponse response =
        post("batch/" + getDatabaseName() + "/sql/")
            .payload(String.format(scriptPayload, script), CONTENT.JSON)
            .getResponse();
    Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);

    final String payload =
        new ODocument().field("command", "select from E").field("mode", "graph").toJSON();
    response =
        post("command/" + getDatabaseName() + "/sql/").payload(payload, CONTENT.JSON).getResponse();

    final ODocument result = new ODocument().fromJSON(response.getEntity().getContent());
    final Map<String, Object> res = result.field("graph");
    final Collection vertices = (Collection) res.get("vertices");
    final Collection edges = (Collection) res.get("edges");

    Assert.assertEquals(2, vertices.size());
    Assert.assertEquals(1, edges.size());
  }

  @Override
  public String getDatabaseName() {
    return "httpgraph";
  }
}
