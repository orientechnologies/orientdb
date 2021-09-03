package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

public class RemoteDeleteIT extends BareBoneBase1ClientTest {

  @Override
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
      OVertex v1 = graph.newVertex("vertextype").save();
      graph.commit();
      graph.begin();
      assertEquals(1, v1.getVersion());

      OVertex v2 = graph.newVertex("vertextype").save();

      graph.commit();
      graph.begin();
      assertEquals(1, v2.getVersion());

      OEdge e = v1.addEdge(v2, "edgetype").save();
      graph.commit();
      graph.begin();
      assertEquals(2, v1.getVersion());
      assertEquals(2, v2.getVersion());

      e.delete();
      graph.commit();
      graph.begin();
      assertFalse(v1.getVertices(ODirection.OUT, "edgetype").iterator().hasNext());
      assertFalse(v2.getVertices(ODirection.IN, "edgetype").iterator().hasNext());
    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down");
      graph.close();
    }
  }

  @Override
  protected String getDatabaseName() {
    return "RemoteDeleteIT";
  }
}
