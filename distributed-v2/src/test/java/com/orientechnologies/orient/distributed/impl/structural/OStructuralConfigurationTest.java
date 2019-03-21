package com.orientechnologies.orient.distributed.impl.structural;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.config.ONodeIdentity;
import com.orientechnologies.orient.server.OSystemDatabase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OStructuralConfigurationTest {

  private OrientDBInternal context;

  @Before
  public void before() {
    context = OrientDBInternal.extract(new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig()));
  }

  @Test
  public void testSimpleInit() {
    OStructuralConfiguration configuration = new OStructuralConfiguration(new OSystemDatabase(context), context,"node1");
    assertNotNull(configuration.getCurrentNodeIdentity());
  }

  @Test
  public void testFirstInitAndReopen() {
    OStructuralConfiguration configuration = new OStructuralConfiguration(new OSystemDatabase(context), context,"node1");
    ONodeIdentity generatedId = configuration.getCurrentNodeIdentity();

    OStructuralConfiguration configuration1 = new OStructuralConfiguration(new OSystemDatabase(context), context,"node1");
    assertEquals(generatedId, configuration1.getCurrentNodeIdentity());
  }

  @Test
  public void testChangeSaveLoad() {
    OStructuralConfiguration configuration = new OStructuralConfiguration(new OSystemDatabase(context), context,"node1");
    ONodeIdentity generatedId = configuration.getCurrentNodeIdentity();
    configuration.getSharedConfiguration().addNode(new OStructuralNodeConfiguration(ONodeIdentity.generate("node2")));
    configuration.save();

    OStructuralConfiguration configuration1 = new OStructuralConfiguration(new OSystemDatabase(context), context,"node1");
    assertEquals(generatedId, configuration1.getCurrentNodeIdentity());
    assertEquals(1, configuration1.getSharedConfiguration().listNodes().size());
  }

  @After
  public void after() {
    context.drop(OSystemDatabase.SYSTEM_DB_NAME, null, null);
    context.close();
  }

}
