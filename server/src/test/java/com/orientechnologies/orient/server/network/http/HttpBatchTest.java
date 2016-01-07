package com.orientechnologies.orient.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Test HTTP "batch" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpBatchTest extends BaseHttpDatabaseTest {
  public void batchUpdate() throws IOException {
    Assert.assertEquals(post("command/" + getDatabaseName() + "/sql/").payload("create class User", CONTENT.TEXT).getResponse()
        .getStatusLine().getStatusCode(), 200);

    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/").payload("insert into User content {\"userID\": \"35862601\"}", CONTENT.TEXT)
            .setUserName("admin").setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);

    String response = EntityUtils.toString(getResponse().getEntity());

    Assert.assertNotNull(response);

    ODocument insertedDocument = ((List<ODocument>) new ODocument().fromJSON(response).field("result")).get(0);

    // TEST UPDATE
    Assert.assertEquals(
        post("batch/" + getDatabaseName() + "/sql/")
            .payload(
                "{\n" + "    \"transaction\": true,\n" + "    \"operations\": [{\n" + "        \"record\": {\n"
                    + "            \"userID\": \"35862601\",\n" + "            \"externalID\": \"35862601\",\n"
                    + "            \"@rid\": \"" + insertedDocument.getIdentity() + "\", \"@class\": \"User\", \"@version\": "
                    + insertedDocument.getVersion() + "\n" + "        },\n" + "        \"type\": \"u\"\n" + "    }]\n" + "}",
                CONTENT.JSON).getResponse().getStatusLine().getStatusCode(), 200);

    // TEST DOUBLE UPDATE
    Assert.assertEquals(
        post("batch/" + getDatabaseName() + "/sql/")
            .payload(
                "{\n" + "    \"transaction\": true,\n" + "    \"operations\": [{\n" + "        \"record\": {\n"
                    + "            \"userID\": \"35862601\",\n" + "            \"externalID\": \"35862601\",\n"
                    + "            \"@rid\": \"" + insertedDocument.getIdentity() + "\", \"@class\": \"User\", \"@version\": "
                    + (insertedDocument.getVersion() + 1) + "\n" + "        },\n" + "        \"type\": \"u\"\n" + "    }]\n" + "}",
                CONTENT.JSON).getResponse().getStatusLine().getStatusCode(), 200);

    // TEST WRONG VERSION ON UPDATE
    Assert.assertEquals(
        post("batch/" + getDatabaseName() + "/sql/")
            .payload(
                "{\n" + "    \"transaction\": true,\n" + "    \"operations\": [{\n" + "        \"record\": {\n"
                    + "            \"userID\": \"35862601\",\n" + "            \"externalID\": \"35862601\",\n"
                    + "            \"@rid\": \"" + insertedDocument.getIdentity() + "\", \"@class\": \"User\", \"@version\": "
                    + (insertedDocument.getVersion() + 1) + "\n" + "        },\n" + "        \"type\": \"u\"\n" + "    }]\n" + "}",
                CONTENT.JSON).getResponse().getStatusLine().getStatusCode(), 409);

    batchWithEmpty();
  }

  private void batchWithEmpty() throws IOException {
    String json = "{\n"
        + "\"operations\": [{\n"
        + "\"type\": \"script\",\n"
        + "\"language\": \"SQL\","
        + "\"script\": \"let $a = select from User limit 2 \\n"
        + "let $b = select sum(foo) from (select from User where name = foo) \\n"
        + "return [$a, $b]\""
        + "}]\n"
        + "}";
    System.out.println(json);
    HttpResponse response = post("batch/" + getDatabaseName() ).payload(
            json,
        CONTENT.TEXT).getResponse();

    Assert.assertEquals(response.getStatusLine().getStatusCode(), 200);
    InputStream stream = response.getEntity().getContent();
    ODocument doc = new ODocument();
    doc.fromJSON(stream);
    stream.close();
    Iterable iterable = doc.field("result");

    Iterator iterator = iterable.iterator();
    Assert.assertTrue(iterator.hasNext());
    iterator.next();
    Assert.assertTrue(iterator.hasNext());
    Object emptyList = iterator.next();
    Assert.assertNotNull(emptyList);
    Assert.assertTrue(emptyList instanceof Iterable);
    Iterator emptyListIterator = ((Iterable) emptyList).iterator();
    Assert.assertFalse(emptyListIterator.hasNext());
  }
    @Override
  public String getDatabaseName() {
    return "httpscript";
  }
}
