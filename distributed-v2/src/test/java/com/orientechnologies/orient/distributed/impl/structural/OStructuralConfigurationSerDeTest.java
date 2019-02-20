package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.server.OSystemDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OStructuralConfigurationSerDeTest {

  private OrientDBInternal context;

  @Before
  public void before() {
    context = OrientDBInternal.extract(new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig()));
  }

  @Test
  public void testDiscSerializationDeserialization() throws IOException {
    OStructuralConfiguration configuration = new OStructuralConfiguration(new OSystemDatabase(context), context);

    OStructuralNodeConfiguration nodeConfiguration = new OStructuralNodeConfiguration(ONodeIdentity.generate());
    OStructuralNodeDatabase database = new OStructuralNodeDatabase(UUID.randomUUID().toString(), "one",
        OStructuralNodeDatabase.NodeMode.ACTIVE);

    nodeConfiguration.addDatabase(database);
    OStructuralNodeDatabase database1 = new OStructuralNodeDatabase(UUID.randomUUID().toString(), "two",
        OStructuralNodeDatabase.NodeMode.ACTIVE);
    nodeConfiguration.addDatabase(database1);
    configuration.getSharedConfiguration().addNode(nodeConfiguration);

    OStructuralNodeConfiguration nodeConfiguration1 = new OStructuralNodeConfiguration(ONodeIdentity.generate());
    OStructuralNodeDatabase database2 = new OStructuralNodeDatabase(UUID.randomUUID().toString(), "one",
        OStructuralNodeDatabase.NodeMode.ACTIVE);
    nodeConfiguration1.addDatabase(database2);

    configuration.getSharedConfiguration().addNode(nodeConfiguration1);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    configuration.serialize(new DataOutputStream(outputStream));

    OStructuralConfiguration configuration1 = new OStructuralConfiguration(new OSystemDatabase(context), context);
    configuration1.deserialize(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));

    assertEquals(configuration1.getCurrentNodeIdentity(), configuration.getCurrentNodeIdentity());
    assertEquals(configuration1.getSharedConfiguration().listDatabases().size(),
        configuration.getSharedConfiguration().listDatabases().size());

  }

  @After
  public void after() {
    context.drop(OSystemDatabase.SYSTEM_DB_NAME, null, null);
    context.close();
  }

}
