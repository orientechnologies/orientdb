package com.orientechnologies.orient.server.distributed.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Test;

public class AlterClassTest extends AbstractServerClusterTest {

  private OClass oClass;

  @Test
  public void test() throws Exception {
    init(2);
    prepare(true);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    oClass = db.getMetadata().getSchema().createClass("AlterPropertyTestClass");
  }

  @Override
  protected String getDatabaseName() {
    return getClass().getSimpleName();
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  @Override
  protected void executeTest() throws Exception {
    ODatabaseDocument db =
        serverInstance.get(0).getServerInstance().openDatabase(getDatabaseName());
    try {
      testAlterCustomAttributeInClass();
      testAlterCustomAttributeWithDotInClass();
    } finally {
      db.close();
    }
  }

  private void testAlterCustomAttributeInClass() {
    oClass.setCustom("customAttribute", "value");
    assertEquals("value", oClass.getCustom("customAttribute"));
  }

  private void testAlterCustomAttributeWithDotInClass() {
    oClass.setCustom("custom.attribute", "value");
    assertEquals("value", oClass.getCustom("custom.attribute"));
  }
}
