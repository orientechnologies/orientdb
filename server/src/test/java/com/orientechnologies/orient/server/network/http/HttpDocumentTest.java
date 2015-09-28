package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test HTTP "query" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpDocumentTest extends BaseHttpDatabaseTest {
  public void create() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:99, \"@version\":100}", CONTENT.JSON).exec();

    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.getVersion(), 1);
    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 99);
    Assert.assertEquals(created.getVersion(), 1);
  }

  public void read() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:99}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 99);
    Assert.assertEquals(created.getVersion(), 1);

    get("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);
    final ODocument updated = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(updated.field("name"), "Jay");
    Assert.assertEquals(updated.field("surname"), "Miner");
    Assert.assertEquals(updated.field("age"), 99);
    Assert.assertEquals(updated.getVersion(), 1);
  }

  public void updateFull() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    created.field("name", "Jay2");
    created.field("surname", "Miner2");
    created.field("age", 1);

    put("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).payload(created.toJSON(),
        CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);
    final ODocument updated = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(updated.field("name"), "Jay2");
    Assert.assertEquals(updated.field("surname"), "Miner2");
    Assert.assertEquals(updated.field("age"), 1);
    Assert.assertEquals(updated.getVersion(), 2);
  }

  public void updateFullNoVersion() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    put("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).payload(
        "{name:'Jay2', surname:'Miner2',age:1}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);
    final ODocument updated = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(updated.field("name"), "Jay2");
    Assert.assertEquals(updated.field("surname"), "Miner2");
    Assert.assertEquals(updated.field("age"), 1);
    Assert.assertEquals(updated.getVersion(), 2);
  }

  public void updateFullBadVersion() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    put("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).payload(
        "{name:'Jay2', surname:'Miner2',age:1, @version: 2}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 409);
  }

  public void updatePartial() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    put("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1) + "?updateMode=partial").payload(
        "{age:1}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);
    final ODocument updated = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(updated.field("name"), "Jay");
    Assert.assertEquals(updated.field("surname"), "Miner");
    Assert.assertEquals(updated.field("age"), 1);
    Assert.assertEquals(updated.getVersion(), 2);
  }

  public void deleteByRid() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    delete("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 204);

    get("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 404);
  }

  public void deleteWithMVCC() throws IOException {
    post("document/" + getDatabaseName()).payload("{name:'Jay', surname:'Miner',age:0}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
    final ODocument created = new ODocument().fromJSON(getResponse().getEntity().getContent());

    Assert.assertEquals(created.field("name"), "Jay");
    Assert.assertEquals(created.field("surname"), "Miner");
    Assert.assertEquals(created.field("age"), 0);
    Assert.assertEquals(created.getVersion(), 1);

    delete("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).payload(created.toJSON(),
        CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 204);

    get("document/" + getDatabaseName() + "/" + created.getIdentity().toString().substring(1)).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 404);
  }

  @Override
  public String getDatabaseName() {
    return "httpdocument";
  }
}
