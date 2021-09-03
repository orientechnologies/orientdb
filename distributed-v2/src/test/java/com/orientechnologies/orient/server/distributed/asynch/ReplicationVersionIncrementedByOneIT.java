package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;

public class ReplicationVersionIncrementedByOneIT extends BareBoneBase1ClientTest {

  @Override
  protected String getDatabaseName() {
    return "ReplicationVersionIncrementedByOneIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    OrientDB orientdb = servers[0].getServer().getContext();
    orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
    ODatabaseDocument graph = orientdb.open(getDatabaseName(), "admin", "admin");
    if (!graph.getMetadata().getSchema().existsClass("vertextype"))
      graph.createClass("vertextype", "V");
    if (!graph.getMetadata().getSchema().existsClass("edgetype"))
      graph.createClass("edgetype", "E");

    graph.begin();

    try {
      OVertex v1 = graph.newVertex("vertextype");
      v1.save();
      graph.commit();
      graph.begin();
      assertEquals(1, v1.getVersion());

      OVertex v2 = graph.newVertex("vertextype");
      v2.save();
      graph.commit();
      graph.begin();
      assertEquals(1, v2.getVersion());

      v1.addEdge(v2, "edgetype");
      v1.save();
      graph.commit();
      graph.begin();
      assertEquals(2, v1.getVersion());
      assertEquals(2, v2.getVersion());
    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down");
      graph.close();
    }
  }
}
