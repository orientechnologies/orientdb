package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import java.util.Iterator;
import org.junit.Assert;
import org.junit.Test;

public class OrientEdgeClassTest {

  @Test
  public void testDeleteEdgeWhileIterate() {
    OrientGraph graph = new OrientGraph("memory:test_graph", "admin", "admin");
    try {
      graph.createEdgeType("Plays");
      OrientVertex player = graph.addVertex(null);
      OrientVertex team = graph.addVertex(null);
      OrientVertex team2 = graph.addVertex(null);
      player.addEdge("plays", team);
      player.addEdge("PlAyS", team2);
      graph.commit();

      // CHECK CASE INSENSITIVE
      Iterator<Edge> team_edges = player.getEdges(Direction.OUT, "pLAys").iterator();
      Assert.assertTrue(team_edges.hasNext());
      team_edges.next();
      Assert.assertTrue(team_edges.hasNext());
      team_edges.next();

      // CHECK CASE SENSITIVE
      team_edges = player.getEdges(Direction.OUT, "Plays").iterator();
      Assert.assertTrue(team_edges.hasNext());
      team_edges.next();
      Assert.assertTrue(team_edges.hasNext());
      team_edges.next();

      ODocument docPlayer = player.getRecord();
      Iterable<OIdentifiable> out = docPlayer.field("out_Plays");
      Assert.assertTrue(out.iterator().hasNext());

      ODocument docTeam = team.getRecord();
      Iterable<OIdentifiable> in = docTeam.field("in_Plays");
      Assert.assertTrue(in.iterator().hasNext());

      graph.getRawGraph().getLocalCache().clear();

      docPlayer = graph.getRawGraph().load(docPlayer.getIdentity());
      out = docPlayer.field("out_Plays");
      Assert.assertTrue(out.iterator().hasNext());

      docTeam = graph.getRawGraph().load(docTeam.getIdentity());
      in = docTeam.field("in_Plays");
      Assert.assertTrue(in.iterator().hasNext());
    } finally {
      graph.drop();
    }
  }
}
