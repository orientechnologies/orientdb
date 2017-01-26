package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OVertex;

public class TestReplicationVersionIncrementedByOne extends BareBoneBase1ClientTest {

  @Override
  protected String getDatabaseName() {
    return "TestReplicationVersionIncrementedByOne";
  }

  protected void dbClient1() {
    ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL());
    if(graph.exists()){
      graph.open("admin", "admin");
    }else{
      graph.create();
      graph.createClass("vertextype","V");
      graph.createClass("edgetype","E");
    }
    graph.begin();

    try {
      OVertex v1 = graph.newVertex("vertextype");
      graph.commit();
      graph.begin();
      assertEquals(1, v1.getVersion());

      OVertex v2 = graph.newVertex("vertextype");
      graph.commit();
      graph.begin();
      assertEquals(1, v2.getVersion());

      v1.addEdge(v2, "edgetype");
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
