package com.orientechnologies.orient.server.distributed.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.setup.ServerRun;
import org.junit.Test;

public class AlterPropertyTest extends AbstractServerClusterTest {

  @Test
  public void test() throws Exception {
    init(2);
    prepare(true);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    OClass oClass = db.getMetadata().getSchema().createClass("AlterPropertyTestClass");

    oClass.createProperty("property", OType.STRING);
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
      // wait till databases will be opened on both servers
      Thread.sleep(1_000);

      testAlterCustomAttributeInProperty(db);
      testAlterCustomAttributeWithDotInProperty(db);
    } finally {
      db.close();
    }
  }

  private void testAlterCustomAttributeInProperty(ODatabaseDocument db) {
    OClass oClass = db.getMetadata().getSchema().getClass("AlterPropertyTestClass");
    OProperty property = oClass.getProperty("property");
    property.setCustom("customAttribute", "value");
    assertEquals("value", property.getCustom("customAttribute"));
  }

  private void testAlterCustomAttributeWithDotInProperty(ODatabaseDocument db) {
    OClass oClass = db.getMetadata().getSchema().getClass("AlterPropertyTestClass");
    OProperty property = oClass.getProperty("property");
    property.setCustom("custom.attribute", "value");
    assertEquals("value", property.getCustom("custom.attribute"));
  }
}
