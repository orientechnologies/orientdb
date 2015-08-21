package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageOperationResult;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@Test(groups = "db")
public class RemoteProtocolCommandsTest extends DocumentDBBaseTest {
  private static final String serverPort = System.getProperty("orient.server.port", "2424");

  @Parameters(value = "url")
  public RemoteProtocolCommandsTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testConnect() throws Exception {
    final OServerAdmin admin = new OServerAdmin("remote:localhost:" + serverPort).connect("root",
        ODatabaseHelper.getServerRootPassword());
    admin.close();
  }

  @Test
  public void testListDatabasesMemoryDB() throws Exception {
    final OServerAdmin admin = new OServerAdmin("remote:localhost").connect("root", ODatabaseHelper.getServerRootPassword());
    try {
      final String plocalDatabaseName = "plocalTestListDatabasesMemoryDB" + Math.random();
      admin.createDatabase(plocalDatabaseName, "graph", "plocal");

      final String memoryDatabaseName = "memoryTestListDatabasesMemoryDB" + Math.random();
      admin.createDatabase(memoryDatabaseName, "graph", "memory");

      final Map<String, String> list = admin.listDatabases();

      Assert.assertTrue(list.containsKey(plocalDatabaseName), "Check plocal db is in list");
      Assert.assertTrue(list.containsKey(memoryDatabaseName), "Check memory db is in list");
    } finally {
      admin.close();
    }
  }

  @Test
  public void testRawCreateWithoutIDTest() {
    OClass clazz = this.database.getMetadata().getSchema().createClass("RidCreationTestClass");
    OStorage storage = this.database.getStorage();
    ODocument doc = new ODocument("RidCreationTestClass");
    doc.field("test", "test");
    ORecordId bad = new ORecordId(-1, -1);
    OStorageOperationResult<OPhysicalPosition> res = storage.createRecord(bad, doc.toStream(), doc.getRecordVersion(),
        ODocument.RECORD_TYPE, OPERATION_MODE.SYNCHRONOUS.ordinal(), null);

    // assertTrue(" the cluster is not valid", bad.clusterId >= 0);
    String ids = "";
    for (int aId : clazz.getClusterIds())
      ids += aId;

    assertTrue(" returned id:" + bad.clusterId + " shoud be one of:" + ids,
        Arrays.binarySearch(clazz.getClusterIds(), bad.clusterId) >= 0);
  }
}
