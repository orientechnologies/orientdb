package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OVertex;

public class ReplicationVersionIncrementedByOneIT extends BareBoneBase1ClientTest {
  private static final OLogger logger =
      OLogManager.instance().logger(ReplicationVersionIncrementedByOneIT.class);

  @Override
  protected String getDatabaseName() {
    return "ReplicationVersionIncrementedByOneIT";
  }

  protected void dbClient1(BareBonesServer[] servers) {
    OrientDB orientdb = servers[0].getServer().getContext();
    orientdb.createIfNotExists(getDatabaseName(), ODatabaseType.PLOCAL);
    ODatabaseDocument graph = orientdb.open(getDatabaseName(), "admin", "adminpwd");
    if (!graph.getMetadata().getSchema().existsClass("vertextype"))
      graph.createClass("vertextype", "V");
    if (!graph.getMetadata().getSchema().existsClass("edgetype"))
      graph.createClass("edgetype", "E");

    graph.begin();

    try {
      OVertex v1 = graph.newVertex("vertextype");
      graph.save(v1);
      graph.commit();
      graph.begin();
      assertEquals(1, v1.getVersion());

      OVertex v2 = graph.newVertex("vertextype");
      graph.save(v2);
      graph.commit();
      graph.begin();
      assertEquals(1, v2.getVersion());

      v1.addEdge(v2, "edgetype");
      graph.save(v1);
      graph.commit();
      graph.begin();
      assertEquals(2, v1.getVersion());
      assertEquals(2, v2.getVersion());
    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      logger.info("Shutting down");
      graph.close();
    }
  }
}
