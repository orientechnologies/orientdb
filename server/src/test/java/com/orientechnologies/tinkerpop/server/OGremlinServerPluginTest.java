package com.orientechnologies.tinkerpop.server;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.tinkerpop.AbstractRemoteTest;
import java.util.concurrent.ExecutionException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 07/09/2017. */
public class OGremlinServerPluginTest extends AbstractRemoteTest {

  @Override
  public void setup() throws Exception {
    super.setup();
    installGremlinServer();
  }

  @Test
  public void shouldAuthenticateWithPlainText() throws Exception {

    MessageSerializer serializer =
        new GryoMessageSerializerV3d0(
            GryoMapper.build().addRegistry(OrientIoRegistry.getInstance()));
    final Cluster cluster =
        Cluster.build().credentials("root", "root").serializer(serializer).create();
    final Client client = cluster.connect();

    try {
      assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
      assertEquals(3, client.submit("1+2").all().get().get(0).getInt());
      assertEquals(4, client.submit("1+3").all().get().get(0).getInt());
    } finally {
      cluster.close();
    }
  }

  @Test
  public void shouldGiveAuthenticationException() throws Exception {
    final Cluster cluster = Cluster.build().credentials("root", "root1").create();
    final Client client = cluster.connect();
    try {
      assertEquals(2, client.submit("1+1").all().get().get(0).getInt());
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof ResponseException);
      Assert.assertEquals("Username and/or password are incorrect", e.getCause().getMessage());
    } finally {
      cluster.close();
    }
  }

  @Test
  public void shouldUseReaderAndGiveExceptionOnWrite() throws Exception {
    final Cluster cluster = Cluster.build().credentials("root", "root").create();
    final Client client = cluster.connect();
    try {
      assertEquals(
          2, client.submit("graph.addVertex(T.label,'Person')").all().get().get(0).getInt());
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof ResponseException);
      Assert.assertEquals(
          "User 'reader' does not have permission to execute the operation 'Create' against the resource: ResourceGeneric [name=SCHEMA, legacyName=database.schema].null\r\n"
              + "\tDB name=\"shouldUseReaderAndGiveExceptionOnWrite\"",
          e.getCause().getMessage());
    } finally {
      cluster.close();
    }
  }

  @Test
  public void shouldCreateAVertexPerson() throws Exception {
    MessageSerializer serializer =
        new GryoMessageSerializerV3d0(
            GryoMapper.build().addRegistry(OrientIoRegistry.getInstance()));
    final Cluster cluster =
        Cluster.build().credentials("root", "root").serializer(serializer).create();
    final Client client = cluster.connect();
    try {
      Vertex vertex =
          client
              .submit("v = graph.addVertex(T.label,'Person','name','John');graph.tx().commit();v")
              .all()
              .get()
              .get(0)
              .getVertex();
      assertEquals("John", vertex.property("name").value());
      assertThat(vertex.id(), instanceOf(ORecordId.class));
      ORecordId id = (ORecordId) vertex.id();
      assertTrue(id.isPersistent());
    } finally {
      cluster.close();
    }
  }

  @Override
  protected void onConfiguration(BaseConfiguration configuration) {

    if (name.getMethodName().equalsIgnoreCase("shouldUseReaderAndGiveExceptionOnWrite")) {
      configuration.addProperty(OrientGraph.CONFIG_USER, "reader");
      configuration.addProperty(OrientGraph.CONFIG_PASS, "reader");
    }
  }
}
