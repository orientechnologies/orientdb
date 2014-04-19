package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.junit.Test;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class TestSharding extends AbstractServerClusterTest {

  protected OrientVertex[] vertices;

  @Test
  public void test() throws Exception {
    init(3);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return "sharding";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sharded-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  protected void executeTest() throws Exception {
    OrientGraphFactory factory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    OrientGraphNoTx graph = factory.getNoTx();

    try {
      final OrientVertexType clientType = graph.createVertexType("Client");

      vertices = new OrientVertex[serverInstance.size()];
      for (int i = 0; i < vertices.length; ++i) {
        clientType.addCluster("client_" + i);

        vertices[i] = graph.addVertex("class:Client,cluster:client_" + i);
        vertices[i].setProperty("name", "shard_" + i);
      }
    } finally {
      graph.shutdown();
    }

    for (int i = 0; i < vertices.length; ++i) {
      OrientGraphFactory f = new OrientGraphFactory("plocal:target/server" + i + "/databases/" + getDatabaseName());
      OrientGraphNoTx g = f.getNoTx();
      try {
        Iterable<OrientVertex> result = g.command(new OCommandSQL("select from Client")).execute();
        Assert.assertTrue(result.iterator().hasNext());

        OrientVertex v = result.iterator().next();

        Assert.assertEquals(v.getProperty("name"), "shard_" + i);

      } finally {
        graph.shutdown();
      }
    }
  }
}
