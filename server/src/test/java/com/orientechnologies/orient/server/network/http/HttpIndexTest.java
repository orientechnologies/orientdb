package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.util.EntityUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Test HTTP "index" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpIndexTest extends BaseHttpDatabaseTest {
  public void create() throws IOException {
    put("index/" + getDatabaseName() + "/ManualIndex/luca").payload("{name:'Harry', surname:'Potter',age:18}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
  }

  public void retrieve() throws IOException {
    get("index/" + getDatabaseName() + "/ManualIndex/jay").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);

    String response = EntityUtils.toString(getResponse().getEntity());
    Assert.assertEquals(response.charAt(0), '[');
    Assert.assertEquals(response.charAt(response.length() - 1), ']');
    response = response.substring(1, response.length() - 1);

    final ODocument jay = new ODocument().fromJSON(response);
    Assert.assertEquals(jay.field("name"), "Jay");
    Assert.assertEquals(jay.field("surname"), "Miner");
    Assert.assertEquals(jay.field("age"), 99);
    Assert.assertEquals(jay.getVersion(), 1);
  }

  public void retrieveNonExistent() throws IOException {
    get("index/" + getDatabaseName() + "/ManualIndex/NonExistent").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 404);
  }

  public void updateKey() throws IOException {
    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload("{name:'Harry', surname:'Potter',age:18}", CONTENT.JSON)
        .exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload("{name:'Harry2', surname:'Potter2',age:182}", CONTENT.JSON)
        .exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    get("index/" + getDatabaseName() + "/ManualIndex/Harry2").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);

    String response = EntityUtils.toString(getResponse().getEntity());
    Assert.assertEquals(response.charAt(0), '[');
    Assert.assertEquals(response.charAt(response.length() - 1), ']');
    response = response.substring(1, response.length() - 1);

    final ODocument jay = new ODocument().fromJSON(response);
    Assert.assertEquals(jay.field("name"), "Harry2");
    Assert.assertEquals(jay.field("surname"), "Potter2");
    Assert.assertEquals(jay.field("age"), 182);
    Assert.assertEquals(jay.getVersion(), 1);
  }

  public void updateValue() throws IOException {
    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload("{name:'Harry', surname:'Potter',age:18}", CONTENT.JSON)
        .exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    get("index/" + getDatabaseName() + "/ManualIndex/Harry2").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);

    String response = EntityUtils.toString(getResponse().getEntity());
    Assert.assertEquals(response.charAt(0), '[');
    Assert.assertEquals(response.charAt(response.length() - 1), ']');
    response = response.substring(1, response.length() - 1);

    ODocument harry = new ODocument().fromJSON(response);

    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload(
        "{name:'Harry3', surname:'Potter3',age:183,@rid:'" + harry.getIdentity() + "',@version:" + harry.getVersion() + "}",
        CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 204);

    get("index/" + getDatabaseName() + "/ManualIndex/Harry2").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);

    response = EntityUtils.toString(getResponse().getEntity());
    Assert.assertEquals(response.charAt(0), '[');
    Assert.assertEquals(response.charAt(response.length() - 1), ']');
    response = response.substring(1, response.length() - 1);

    harry = new ODocument().fromJSON(response);
    Assert.assertEquals(harry.field("name"), "Harry3");
    Assert.assertEquals(harry.field("surname"), "Potter3");
    Assert.assertEquals(harry.field("age"), 183);
    Assert.assertEquals(harry.getVersion(), 2);
  }

  public void updateValueMVCCError() throws IOException {
    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload("{name:'Harry', surname:'Potter',age:18}", CONTENT.JSON)
        .exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);

    get("index/" + getDatabaseName() + "/ManualIndex/Harry2").exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 200);

    String response = EntityUtils.toString(getResponse().getEntity());
    Assert.assertEquals(response.charAt(0), '[');
    Assert.assertEquals(response.charAt(response.length() - 1), ']');
    response = response.substring(1, response.length() - 1);

    ODocument harry = new ODocument().fromJSON(response);

    put("index/" + getDatabaseName() + "/ManualIndex/Harry2").payload(
        "{name:'Harry3', surname:'Potter3',age:183,@rid:'" + harry.getIdentity() + "'}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 409);
  }

  @Override
  protected void onAfterDatabaseCreated() throws Exception {
    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql").payload("create index ManualIndex DICTIONARY STRING", CONTENT.TEXT)
            .getResponse().getStatusLine().getStatusCode(), 200);

    put("index/" + getDatabaseName() + "/ManualIndex/jay").payload("{name:'Jay', surname:'Miner',age:99}", CONTENT.JSON).exec();
    Assert.assertEquals(getResponse().getStatusLine().getStatusCode(), 201);
  }

  @Override
  public String getDatabaseName() {
    return "httpindex";
  }
}
