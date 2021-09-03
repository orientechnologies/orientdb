package com.orientechnologies.orient.distributed.impl.structural;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBConfigBuilder;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.distributed.OrientDBDistributed;
import com.orientechnologies.orient.server.OSystemDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OStructuralConfigurationTest {

  private OrientDBDistributed context;

  @Before
  public void before() {
    OrientDBConfigBuilder builder = OrientDBConfig.builder();
    builder.getNodeConfigurationBuilder().setNodeName("node1").setQuorum(2);
    context = (OrientDBDistributed) OrientDBInternal.distributed("./target/", builder.build());
  }

  @Test
  public void testSimpleInit() {
    OStructuralConfiguration configuration =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    assertNotNull(configuration.getCurrentNodeIdentity());
  }

  @Test
  public void testFirstInitAndReopen() {
    OStructuralConfiguration configuration =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    ONodeIdentity generatedId = configuration.getCurrentNodeIdentity();

    OStructuralConfiguration configuration1 =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    assertEquals(generatedId, configuration1.getCurrentNodeIdentity());
  }

  @Test
  public void testChangeSaveLoad() {
    OStructuralConfiguration configuration =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    ONodeIdentity generatedId = configuration.getCurrentNodeIdentity();
    OStructuralSharedConfiguration config = configuration.modifySharedConfiguration();
    config.addNode(new OStructuralNodeConfiguration(ONodeIdentity.generate("node2")));
    configuration.update(config);

    OStructuralConfiguration configuration1 =
        new OStructuralConfiguration(new OSystemDatabase(context), context);
    assertEquals(generatedId, configuration1.getCurrentNodeIdentity());
    assertEquals(1, configuration1.readSharedConfiguration().listNodes().size());
  }

  @After
  public void after() {
    context.drop(OSystemDatabase.SYSTEM_DB_NAME, null, null);
    context.close();
  }
}
