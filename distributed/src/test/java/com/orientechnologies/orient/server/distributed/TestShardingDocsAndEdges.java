package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestShardingDocsAndEdges extends AbstractServerClusterTest {

  protected final static int  SERVERS        = 2;
  private static final String clusterNodeUSA = "user_usa";
  private static final String clusterNodeEUR = "user_eur";
  private static int          testNumber     = 0;
  private ODatabaseDocumentTx USA;
  private ODatabaseDocumentTx EUR;

  @Test
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return "sharding2";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sharded-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  protected void onAfterDatabaseCreation(OrientBaseGraph db) {
    db.command(new OCommandSQL("ALTER DATABASE CUSTOM useLightweightEdges = true")).execute();
    db.command(new OCommandSQL("ALTER DATABASE MINIMUMCLUSTERS 2")).execute();
    db.command(new OCommandSQL("create class User extends V")).execute();
    db.command(new OCommandSQL("create property User.name string")).execute();
    db.command(new OCommandSQL("alter cluster user_0 name user_usa")).execute();
    db.command(new OCommandSQL("alter cluster user_1 name user_eur")).execute();
    db.command(new OCommandSQL("create class Follows extends E")).execute();
  }

  @Override
  protected void executeTest() throws Exception {
    EUR = new ODatabaseDocumentTx("plocal:target/server0/databases/" + getDatabaseName());
    USA = new ODatabaseDocumentTx("plocal:target/server1/databases/" + getDatabaseName());

    Set<String> queryResult;

    execute(USA, "insert into cluster:" + clusterNodeUSA + " set name = 'mike'");
    Thread.sleep(1000);

    // test 0
    queryResult = execute(USA, "select from cluster:" + clusterNodeUSA);
    compare(queryResult, new String[] { "mike" });

    queryResult = execute(USA, "select from cluster:" + clusterNodeEUR);
    compare(queryResult, new String[] {});

    queryResult = execute(USA, "select from User");
    compare(queryResult, new String[] { "mike" });

    queryResult = execute(EUR, "select from cluster:" + clusterNodeUSA);
    compare(queryResult, new String[] { "mike" });

    queryResult = execute(EUR, "select from cluster:" + clusterNodeEUR);
    compare(queryResult, new String[] {});

    queryResult = execute(EUR, "select from User");
    compare(queryResult, new String[] { "mike" });

    execute(EUR, "insert into cluster:" + clusterNodeEUR + " set name = 'phoebe'");
    Thread.sleep(1000);

    // test 6
    queryResult = execute(USA, "select from cluster:" + clusterNodeUSA);
    compare(queryResult, new String[] { "mike" });

    queryResult = execute(USA, "select from cluster:" + clusterNodeEUR);
    compare(queryResult, new String[] { "phoebe" });

    queryResult = execute(USA, "select from User");
    compare(queryResult, new String[] { "mike", "phoebe" });

    queryResult = execute(EUR, "select from cluster:" + clusterNodeUSA);
    compare(queryResult, new String[] { "mike" });

    queryResult = execute(EUR, "select from cluster:" + clusterNodeEUR);
    compare(queryResult, new String[] { "phoebe" });

    queryResult = execute(EUR, "select from User");
    compare(queryResult, new String[] { "mike", "phoebe" });

    /*
     * verify that 'select from V returns' the same as 'select from User' on both nodes
     */
    // test 12
    queryResult = execute(USA, "select from V");
    compare(queryResult, new String[] { "mike", "phoebe" });
    // test 13
    queryResult = execute(EUR, "select from V");
    compare(queryResult, new String[] { "mike", "phoebe" });

    // LINE A
    execute(USA, "create edge Follows from (select from User where name = 'mike') to (select from User where name = 'phoebe')");

    // ...
  }

  static Set<String> execute(ODatabaseDocument db, String command) throws InterruptedException {
    System.out.println(command);
    Set<String> resultSet = new HashSet();
    db.open("admin", "admin");
    try {
      Object o = db.command(new OCommandSQL(command)).execute();
      if (o instanceof List) {
        List<ODocument> resultList = (List) o;
        for (OIdentifiable d : resultList) {
          resultSet.add((String) ((ODocument) d.getRecord()).field("name"));
        }
      }
    } finally {
      db.activateOnCurrentThread();
      db.close();
    }
    return resultSet;
  }

  static void compare(Set<String> result, String[] expected) {
    boolean equal;

    if (result.size() != expected.length) {
      equal = false;
    } else {
      equal = true;
      for (String s : expected) {
        equal &= result.contains(s);
      }
    }

    if (equal) {
      System.out.println(testNumber + " : ok");
    } else {
      System.out.println(testNumber + " : ko -> result : " + result.toString());
    }
    testNumber++;

  }
}
