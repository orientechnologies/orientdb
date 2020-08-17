package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Ignore;
import org.junit.Test;

public class ShardingDocsAndEdgesIT extends AbstractServerClusterTest {

  protected static final int SERVERS = 2;
  private static final String clusterNodeUSA = "client-type_usa";
  private static final String clusterNodeEUR = "client-type_eur";
  private static int testNumber = 0;
  private ODatabaseDocument USA;
  private ODatabaseDocument EUR;

  @Test
  @Ignore
  public void test() throws Exception {
    init(SERVERS);
    prepare(false);
    execute();
  }

  @Override
  protected String getDatabaseName() {
    return "ShardingDocsAndEdgesIT";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sharded-dserver-config-" + server.getServerId() + ".xml";
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    db.command(new OCommandSQL("ALTER DATABASE CUSTOM useLightweightEdges = true")).execute();
    db.command(new OCommandSQL("ALTER DATABASE MINIMUMCLUSTERS 2")).execute();
    db.command(new OCommandSQL("create class `Client-Type` extends V")).execute();
    db.command(new OCommandSQL("create property `Client-Type`.name string")).execute();
    db.command(new OCommandSQL("alter cluster `Client-Type`   name `client-type_usa`")).execute();
    db.command(new OCommandSQL("alter cluster `client-type_1` name `client-type_eur`")).execute();
    db.command(new OCommandSQL("create class Follows extends E")).execute();
  }

  @Override
  protected void executeTest() throws Exception {
    EUR =
        serverInstance
            .get(0)
            .getServerInstance()
            .getContext()
            .open(getDatabaseName(), "admin", "admin");
    USA =
        serverInstance
            .get(1)
            .getServerInstance()
            .getContext()
            .open(getDatabaseName(), "admin", "admin");

    Set<String> queryResult;

    execute(USA, "insert into `cluster:" + clusterNodeUSA + "` set name = 'mike'");
    Thread.sleep(1000);

    // test 0
    queryResult = execute(USA, "select from `cluster:" + clusterNodeUSA + "`");
    compare(queryResult, new String[] {"mike"});

    queryResult = execute(USA, "select from `cluster:" + clusterNodeEUR + "`");
    compare(queryResult, new String[] {});

    queryResult = execute(USA, "select from `Client-Type`");
    compare(queryResult, new String[] {"mike"});

    queryResult = execute(EUR, "select from `cluster:" + clusterNodeUSA + "`");
    compare(queryResult, new String[] {"mike"});

    queryResult = execute(EUR, "select from `cluster:" + clusterNodeEUR + "`");
    compare(queryResult, new String[] {});

    queryResult = execute(EUR, "select from `Client-Type`");
    compare(queryResult, new String[] {"mike"});

    execute(EUR, "insert into `cluster:" + clusterNodeEUR + "` set name = 'phoebe'");
    Thread.sleep(1000);

    // test 6
    queryResult = execute(USA, "select from `cluster:" + clusterNodeUSA + "`");
    compare(queryResult, new String[] {"mike"});

    queryResult = execute(USA, "select from `cluster:" + clusterNodeEUR + "`");
    compare(queryResult, new String[] {"phoebe"});

    queryResult = execute(USA, "select from `Client-Type`");
    compare(queryResult, new String[] {"mike", "phoebe"});

    queryResult = execute(EUR, "select from `cluster:" + clusterNodeUSA + "`");
    compare(queryResult, new String[] {"mike"});

    queryResult = execute(EUR, "select from `cluster:" + clusterNodeEUR + "`");
    compare(queryResult, new String[] {"phoebe"});

    queryResult = execute(EUR, "select from `Client-Type`");
    compare(queryResult, new String[] {"mike", "phoebe"});

    /*
     * verify that 'select from V returns' the same as 'select from Client-Type' on both nodes
     */
    // test 12
    queryResult = execute(USA, "select from V");
    compare(queryResult, new String[] {"mike", "phoebe"});
    // test 13
    queryResult = execute(EUR, "select from V");
    compare(queryResult, new String[] {"mike", "phoebe"});

    // LINE A
    execute(
        USA,
        "create edge Follows from (select from `Client-Type` where name = 'mike') to (select from `Client-Type` where name = 'phoebe')");
    USA.close();
    EUR.close();
  }

  static Set<String> execute(ODatabaseDocument db, String command) throws InterruptedException {
    System.out.println(command);
    Set<String> resultSet = new HashSet();

    // CREATE A GRAPH TO MANIPULATE ELEMENTS

    db.activateOnCurrentThread();

    Object o = db.command(new OCommandSQL(command)).execute();
    if (o instanceof List) {
      List<ODocument> resultList = (List) o;
      for (OIdentifiable d : resultList) {
        if (d.getRecord() instanceof ODocument) {
          resultSet.add((String) ((ODocument) d.getRecord()).field("name"));
        }
      }
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
