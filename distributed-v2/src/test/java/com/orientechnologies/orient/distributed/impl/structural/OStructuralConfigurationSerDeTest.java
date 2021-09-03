package com.orientechnologies.orient.distributed.impl.structural;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.OSystemDatabase;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OStructuralConfigurationSerDeTest {

  private OrientDBDistributed context;

  @Before
  public void before() {
    OrientDBConfigBuilder builder = OrientDBConfig.builder();
    builder.getNodeConfigurationBuilder().setNodeName("node1").setQuorum(2);
    context = (OrientDBDistributed) OrientDBInternal.distributed("./target/", builder.build());
  }

  @Test
  public void testDiscSerializationDeserialization() throws IOException {
    OStructuralConfiguration configuration =
        new OStructuralConfiguration(new OSystemDatabase(context), context);

    OStructuralNodeConfiguration nodeConfiguration =
        new OStructuralNodeConfiguration(ONodeIdentity.generate("node2"));
    OStructuralNodeDatabase database =
        new OStructuralNodeDatabase(
            UUID.randomUUID(), "one", OStructuralNodeDatabase.NodeMode.ACTIVE);

    nodeConfiguration.addDatabase(database);
    OStructuralNodeDatabase database1 =
        new OStructuralNodeDatabase(
            UUID.randomUUID(), "two", OStructuralNodeDatabase.NodeMode.ACTIVE);
    nodeConfiguration.addDatabase(database1);
    OStructuralSharedConfiguration config = configuration.modifySharedConfiguration();
    config.addNode(nodeConfiguration);
    configuration.update(config);

    OStructuralNodeConfiguration nodeConfiguration1 =
        new OStructuralNodeConfiguration(ONodeIdentity.generate("node3"));
    OStructuralNodeDatabase database2 =
        new OStructuralNodeDatabase(
            UUID.randomUUID(), "one", OStructuralNodeDatabase.NodeMode.ACTIVE);
    nodeConfiguration1.addDatabase(database2);

    OStructuralSharedConfiguration config1 = configuration.modifySharedConfiguration();
    config1.addNode(nodeConfiguration1);
    configuration.update(config1);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    configuration.discSerialize(new DataOutputStream(outputStream));

    OStructuralConfiguration configuration1 =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    configuration1.discDeserialize(
        new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));

    assertEquals(configuration1.getCurrentNodeIdentity(), configuration.getCurrentNodeIdentity());
    assertEquals(
        configuration1.readSharedConfiguration().listNodes().size(),
        configuration.readSharedConfiguration().listNodes().size());
  }

  @After
  public void after() {
    context.drop(OSystemDatabase.SYSTEM_DB_NAME, null, null);
    context.close();
  }
}
