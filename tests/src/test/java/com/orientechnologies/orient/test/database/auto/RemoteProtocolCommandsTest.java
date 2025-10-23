package com.orientechnologies.orient.test.database.auto;

import java.util.List;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
@Test(groups = "db")
public class RemoteProtocolCommandsTest extends DocumentDBBaseTest {
  private static final String serverPort = System.getProperty("orient.server.port", "2424");

  @Parameters(value = "url")
  public RemoteProtocolCommandsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final Random random = new Random();

    final String plocalDatabaseName = "plocalTestListDatabasesMemoryDB" + random.nextInt();
    baseContext.execute("create database ? plocal", plocalDatabaseName);

    final String memoryDatabaseName = "memoryTestListDatabasesMemoryDB" + random.nextInt();
    baseContext.execute("create database ? plocal", memoryDatabaseName);

    List<String> list = baseContext.list();

    Assert.assertTrue(list.contains(plocalDatabaseName), "Check plocal db is in list");
    Assert.assertTrue(list.contains(memoryDatabaseName), "Check memory db is in list");
  }
}
