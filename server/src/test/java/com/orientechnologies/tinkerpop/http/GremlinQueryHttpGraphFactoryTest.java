package com.orientechnologies.tinkerpop.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpResponse;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 07/02/17. */
public class GremlinQueryHttpGraphFactoryTest extends BaseGremlinHttpGraphFactoryTest {

  @Test
  public void simpleHttpGremlinQuery() throws IOException {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    HttpResponse post = post("command/" + getDatabaseName() + "/gremlin/", Optional.of("g.V()"));

    String body = asString(post.getEntity().getContent());

    Assert.assertEquals(body, post.getStatusLine().getStatusCode(), 200);

    ODocument entry = asDocument(body);

    final Collection<ODocument> res = entry.field("result");

    Assert.assertEquals(res.size(), 2);
  }

  @Test
  public void simpleHttpGremlinCountQuery() throws IOException {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    HttpResponse post =
        post("command/" + getDatabaseName() + "/gremlin/", Optional.of("g.V().count()"));

    String body = asString(post.getEntity().getContent());

    Assert.assertEquals(body, post.getStatusLine().getStatusCode(), 200);

    ODocument entry = asDocument(body);

    final Collection<Map> res = entry.field("result");

    Assert.assertEquals(res.size(), 1);

    Map next = res.iterator().next();

    Assert.assertEquals(new Long(2).intValue(), next.get("value"));
  }
}
