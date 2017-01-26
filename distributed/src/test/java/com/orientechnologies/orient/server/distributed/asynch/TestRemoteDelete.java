package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

public class TestRemoteDelete extends BareBoneBase1ClientTest {

  @Override
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
    return "TestRemoteDelete";
  }

}
