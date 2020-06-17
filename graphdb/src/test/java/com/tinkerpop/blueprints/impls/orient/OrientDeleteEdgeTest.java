package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

public class OrientDeleteEdgeTest {

  @Test
  public void testDeleteEdgeWhileIterate() {
    OrientGraph graph = new OrientGraph("memory:test_graph", "admin", "admin");
    try {
      graph.createEdgeType("Plays");
      OrientVertex player = graph.addVertex(null);
      OrientVertex team = graph.addVertex(null);
      player.addEdge("plays", team);
      graph.commit();

      Iterator<Edge> team_edges = player.getEdges(Direction.OUT, "plays").iterator();
      while (team_edges.hasNext()) {
        final Edge edge = team_edges.next();
        graph.removeEdge(edge);
      }

      ODocument docPlayer = player.getRecord();
      Iterable<OIdentifiable> out = docPlayer.field("out_Plays");
      Assert.assertFalse(out.iterator().hasNext());

      ODocument docTeam = team.getRecord();
      Iterable<OIdentifiable> in = docTeam.field("in_Plays");
      Assert.assertFalse(in.iterator().hasNext());

      graph.getRawGraph().getLocalCache().clear();

      docPlayer = graph.getRawGraph().load(docPlayer.getIdentity());
      out = docPlayer.field("out_Plays");
      Assert.assertFalse(out.iterator().hasNext());

      docTeam = graph.getRawGraph().load(docTeam.getIdentity());
      in = docTeam.field("in_Plays");
      Assert.assertFalse(in.iterator().hasNext());
    } finally {
      graph.drop();
    }
  }
}
