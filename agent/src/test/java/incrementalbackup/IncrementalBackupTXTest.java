/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package incrementalbackup;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * It tests the behaviour of the incremental backup with through the following steps:
 *
 * - two graph databases (TX) primary db and secondary db and n threads (now set to 5) that execute writes on the primary db
 * - 1st operation: inserting 5000 triples for each thread (with 5 threads: 25000 triples, 50000 vertices, 25000 edges)
 * - 1st incremental backup
 * - 1st restore on the secondary db
 * - 2nd operation: inserting 20000 triples for each thread (with 5 threads: 100000 triples, 200000 vertices, 100000 edges)
 * - 2nd incremental backup
 * - 3rd operation: updating all the vertices
 * - 4th operation: inserting 20000 triples for each thread (with 5 threads: 100000 triples, 200000 vertices, 100000 edges)
 * - 5th operation: deleting the last 10 added vertices
 * - 3rd incremental backup
 * - restore of the primary db backup on the secondary db
 * - comparing primary db and secondary db checking if they are equal
 *
 */
public class IncrementalBackupTXTest extends AbstractBackupTest {

  private OrientBaseGraph primaryGraph;
  private OrientBaseGraph secondaryGraph;
  private final String primaryDbPath =  "target/primarydb_tx";
  private final String secondaryDbPath =  "target/secondarydb_tx";
  private final String primaryDbURL =  "plocal:" + this.primaryDbPath;
  private final String secondaryDbURL =  "plocal:" + this.secondaryDbPath;
  private final String backupPath =  "target/backup";


  @Before
  public void setUp() {
    primaryGraph = new OrientGraphNoTx(this.primaryDbURL);
    secondaryGraph = new OrientGraphNoTx(this.secondaryDbURL);
    incrementalVerticesIdForThread = new int[numberOfThreads];
    for(int i=0; i < this.numberOfThreads; i++) {
      this.incrementalVerticesIdForThread[i] = 0;
    }

    try {

      // initial schema
      OrientVertexType userType = primaryGraph.createVertexType("User");
      userType.createProperty("name", OType.STRING);
      userType.createProperty("updated", OType.BOOLEAN);
      userType.createIndex("User.index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "name");

      OrientVertexType productType = primaryGraph.createVertexType("Product");
      productType.createProperty("name", OType.STRING);
      productType.createProperty("updated", OType.BOOLEAN);
      productType.createIndex("Product.index", OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, "name");

      OrientEdgeType edgeType = primaryGraph.createEdgeType("bought");
      edgeType.createProperty("purchaseDate", OType.DATE);
      edgeType.createProperty("updated", OType.BOOLEAN);


    } catch(Exception e) {
      e.printStackTrace();
      // cleaning all the directories
      this.cleanDirectories();
    } finally {
      primaryGraph.shutdown();
      secondaryGraph.shutdown();
    }
  }

  @Override
  protected String getDatabaseName() {
    return null;
  }

  @Test
  public void testIncrementalBackup() {

    try {
      primaryGraph = new OrientGraph(this.primaryDbURL);
      secondaryGraph = new OrientGraph(this.secondaryDbURL);

      ODatabaseRecordThreadLocal.INSTANCE.set(primaryGraph.getRawGraph());
      this.banner("1st op. - Inserting 25000 triples (50000 vertices, 25000 edges)");
      this.executeWrites(this.primaryDbURL, 5000);
      List<ODocument> result = primaryGraph.getRawGraph().query(new OSQLSynchQuery<OIdentifiable>("select count(*) from V"));
      assertEquals(25000 * 2, ((Number) result.get(0).field("count")).intValue());
      assertEquals(25000, numberOfThreads * 5000);
      assertEquals(5000, incrementalVerticesIdForThread[0]);
      assertEquals(5000, incrementalVerticesIdForThread[1]);
      assertEquals(5000, incrementalVerticesIdForThread[2]);
      assertEquals(5000, incrementalVerticesIdForThread[3]);
      assertEquals(5000, incrementalVerticesIdForThread[4]);

      // first backup
      primaryGraph.getRawGraph().incrementalBackup(this.backupPath);
      // first restore on the secondary db
      ODatabaseRecordThreadLocal.INSTANCE.set(secondaryGraph.getRawGraph());
      secondaryGraph.getRawGraph().incrementalRestore(this.backupPath);
      ODatabaseRecordThreadLocal.INSTANCE.set(primaryGraph.getRawGraph());

      // insert other vertices (20k for each thread)
      this.banner("2nd op. - Inserting 100000 triples (200000 vertices, 100000 edges)");
      this.executeWrites(this.primaryDbURL, 20000);
      result = primaryGraph.getRawGraph().query(new OSQLSynchQuery<OIdentifiable>("select count(*) from V"));
      assertEquals(125000 * 2, ((Number) result.get(0).field("count")).intValue());

      // second backup
      primaryGraph.getRawGraph().incrementalBackup(this.backupPath);

      // update vertices (set updated = true)
      this.banner("3rd op. - Updating all the vertices (250000 vertices)");
      int i = 1;
      for (Vertex v : primaryGraph.getVertices()) {
        this.updateVertex(primaryGraph, (OrientVertex) v);
        if(i % 10000 == 0) {
          System.out.println("\nUpdated successfully " + i + "/250000 records so far");
        }
        i++;
      }
      result = primaryGraph.getRawGraph().query(new OSQLSynchQuery<OIdentifiable>("select count(*) from V where updated = true"));
      assertEquals(125000 * 2, ((Number) result.get(0).field("count")).intValue());

      // insert vertices
      this.banner("4th op. - Inserting 100000 triples (200000 vertices, 100000 edges)");
      this.executeWrites(this.primaryDbURL, 20000);
      result = primaryGraph.getRawGraph().query(new OSQLSynchQuery<OIdentifiable>("select count(*) from V"));
      assertEquals(225000 * 2, ((Number) result.get(0).field("count")).intValue());

      // delete vertices (where updated = true)
      this.banner("5th op. - Deleting the last 10 vertices inserted");
      try {
        primaryGraph
            .command(new OCommandSQL("delete vertex from User where name = 'User-t0-v" + (this.incrementalVerticesIdForThread[0]-1) + "'"
            + " or name = 'User-t1-v" + (this.incrementalVerticesIdForThread[1]-1) + "'"
            + " or name = 'User-t2-v" + (this.incrementalVerticesIdForThread[2]-1) + "'"
            + " or name = 'User-t3-v" + (this.incrementalVerticesIdForThread[3]-1) + "'"
            + " or name = 'User-t4-v" + (this.incrementalVerticesIdForThread[4]-1) + "'")).execute();
        primaryGraph.command(new OCommandSQL("delete vertex from Product where name = 'Product-t0-v" + (this.incrementalVerticesIdForThread[0]-1) + "'"
            + " or name = 'Product-t1-v" + (this.incrementalVerticesIdForThread[1]-1) + "'"
            + " or name = 'Product-t2-v" + (this.incrementalVerticesIdForThread[2]-1) + "'"
            + " or name = 'Product-t3-v" + (this.incrementalVerticesIdForThread[3]-1) + "'"
            + " or name = 'Product-t4-v" + (this.incrementalVerticesIdForThread[4]-1) + "'")).execute();
        primaryGraph.commit();
      } catch (Exception e) {
        primaryGraph.rollback();
      }

      result = primaryGraph.getRawGraph().query(new OSQLSynchQuery< OIdentifiable>("select count(*) from V"));
      assertEquals(449990, ((Number) result.get(0).field("count")).intValue());

      // third backup
      primaryGraph.getRawGraph().incrementalBackup(this.backupPath);
      // restore on the secondary db
      ODatabaseRecordThreadLocal.INSTANCE.set(secondaryGraph.getRawGraph());
      secondaryGraph.getRawGraph().incrementalRestore(this.backupPath);

      // check the backup
      this.banner("Checking consistency of the incremental backup and restore operations");
      this.databaseAreEqual();

    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(primaryGraph.getRawGraph());
      primaryGraph.shutdown();
      ODatabaseRecordThreadLocal.INSTANCE.set(secondaryGraph.getRawGraph());
      secondaryGraph.shutdown();
      // cleaning all the directories
      this.cleanDirectories();
    }

  }

  private void cleanDirectories() {
    OFileUtils.deleteRecursively(new File(this.primaryDbPath));
    OFileUtils.deleteRecursively(new File(this.secondaryDbPath));
    OFileUtils.deleteRecursively(new File(this.backupPath));
  }


  private void databaseAreEqual() {

    OrientGraphNoTx graph1 = new OrientGraphNoTx(this.primaryDbURL);
    OrientGraphNoTx graph2 = new OrientGraphNoTx(this.secondaryDbURL);

    // Checking number of vertices and edges in both the graphs
    ODatabaseRecordThreadLocal.INSTANCE.set(graph1.getRawGraph());
    List<ODocument> result = graph1.getRawGraph().query(new OSQLSynchQuery< OIdentifiable>("select count(*) from V"));
    int numberVerticesGraph1 = ((Number) result.get(0).field("count")).intValue();
    result = graph1.getRawGraph().query(new OSQLSynchQuery< OIdentifiable>("select count(*) from E"));
    int numberEdgesGraph1 = ((Number) result.get(0).field("count")).intValue();

    ODatabaseRecordThreadLocal.INSTANCE.set(graph2.getRawGraph());
    result = graph2.getRawGraph().query(new OSQLSynchQuery< OIdentifiable>("select count(*) from V"));
    int numberVerticesGraph2 = ((Number) result.get(0).field("count")).intValue();
    result = graph2.getRawGraph().query(new OSQLSynchQuery< OIdentifiable>("select count(*) from E"));
    int numberEdgesGraph2 = ((Number) result.get(0).field("count")).intValue();

    assertEquals(numberVerticesGraph1, numberVerticesGraph2);
    assertEquals(numberEdgesGraph1, numberEdgesGraph2);

    // Comparing each single vertex and edge
    Vertex currentVertexGraph2 = null;
    Edge outEdgeV1 = null;
    Edge outEdgeV2 = null;
    List<Vertex> graph1Vertices = new ArrayList<Vertex>();
    List<Edge> outEdges1 = new ArrayList<Edge>();
    List<Edge> outEdges2 = new ArrayList<Edge>();

    for(Vertex v: graph1.getVertices()) {
      graph1Vertices.add(v);
    }
    assertEquals(graph1Vertices.size(), numberVerticesGraph1);

    List<Vertex> queryVertices = new ArrayList<Vertex>();
    ODatabaseRecordThreadLocal.INSTANCE.set(graph2.getRawGraph());

    int comparedVertices = 0;
    int comparedEdges = 0;
    for(Vertex v1: graph1Vertices) {

      for(Vertex v2: graph2.getVertices("name",v1.getProperty("name"))) {
        queryVertices.add(v2);
      }

      assertEquals(1, queryVertices.size());
      currentVertexGraph2 = queryVertices.get(0);
      assertEquals(v1, currentVertexGraph2);
      comparedVertices++;

      queryVertices.clear();

      // Comparing out edges

      for(Edge e: v1.getEdges(Direction.OUT)) {
        outEdges1.add(e);
      }
      for(Edge e: currentVertexGraph2.getEdges(Direction.OUT)) {
        outEdges2.add(e);
      }
      if(outEdges1.size() > 0 && outEdges2.size() > 0) {
        assertEquals(1, outEdges1.size());
        assertEquals(1, outEdges2.size());
        outEdgeV1 = outEdges1.get(0);
        outEdgeV2 = outEdges2.get(0);
        assertEquals(outEdgeV1, outEdgeV2);
        comparedEdges++;
      }
      else {
        assertEquals(0, outEdges1.size());
        assertEquals(0, outEdges2.size());
      }
      outEdges1.clear();
      outEdges2.clear();

      if(comparedVertices % 10000 == 0) {
        System.out.println("\nCompared successfully " + comparedVertices + "/" + numberVerticesGraph1 + " records and " + comparedEdges + "/" + numberEdgesGraph1 + " edges so far");
      }
    }
    System.out.println("\nCompared successfully " + comparedVertices + "/" + numberVerticesGraph1 + " records and " + comparedEdges + "/" + numberEdgesGraph1 + " edges so far");

    graph1.shutdown();
    graph2.shutdown();
  }

}
