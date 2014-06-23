package com.orientechnologies.orient.test.database.auto;

import java.util.Map;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test(groups = "db")
public class RemoteProtocolCommandsTest {

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final OServerAdmin admin = new OServerAdmin("remote:localhost").connect("root", ODatabaseHelper.getServerRootPassword());

    final String plocalDatabaseName = "plocalTestListDatabasesMemoryDB" + Math.random();
    admin.createDatabase(plocalDatabaseName, "graph", "plocal");

    final String memoryDatabaseName = "memoryTestListDatabasesMemoryDB" + Math.random();
    admin.createDatabase(memoryDatabaseName, "graph", "memory");

    final Map<String, String> list = admin.listDatabases();

    Assert.assertTrue("Check plocal db is in list", list.containsKey(plocalDatabaseName));
    Assert.assertTrue("Check memory db is in list", list.containsKey(memoryDatabaseName));
  }
}
