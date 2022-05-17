package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.AbstractRemoteTest;
import java.util.Iterator;
import org.junit.Test;

public class CountRelationshipGraphTest extends AbstractRemoteTest {

  private OrientDB orientdb;
  private int old;

  public void setup() throws Exception {
    old = OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();
    OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
    super.setup();
    orientdb =
        new OrientDB(
            "remote:localhost",
            "root",
            "root",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1)
                .build());
  }

  public void teardown() {
    OGlobalConfiguration.INDEX_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(old);
    super.teardown();
  }

  @Test
  public void test() throws Exception {
    ODatabaseSession g = orientdb.open(name.getMethodName(), "admin", "admin");
    g.begin();
    OVertex vertex1 = g.newVertex("V");
    vertex1.save();
    OVertex vertex2 = g.newVertex("V");
    vertex2.save();
    g.commit();

    int version = vertex1.getProperty("@version");
    assertEquals(0, countEdges(vertex1, ODirection.OUT));
    assertEquals(0, countEdges(vertex1, ODirection.OUT));
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Version: 1 vertex1 out: 0 vertex2 in: 0
     */

    g.begin();
    vertex1.addEdge(vertex2);
    vertex1.save();

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Pre-commit:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Pre-commit: Version: 1 vertex1 out: 1 vertex2 in: 1
     */

    g.commit();

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Post-commit:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Post-commit: Version: 2 vertex1 out: 0 <- INCORRECT vertex2 in: 0 <- INCORRECT
     */

    g.close();

    g = orientdb.open(name.getMethodName(), "admin", "admin");
    vertex1 = g.load(vertex1.getIdentity());
    vertex2 = g.load(vertex2.getIdentity());

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Reload in new transaction:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Reload in new transaction: Version: 2 vertex1 out: 1 vertex2 in: 1
     */
  }

  private int countEdges(OVertex v, ODirection dir) throws Exception {
    int c = 0;
    Iterator it = v.getEdges(dir).iterator();
    while (it.hasNext()) {
      c++;
      it.next();
    }
    return c;
  }
}
