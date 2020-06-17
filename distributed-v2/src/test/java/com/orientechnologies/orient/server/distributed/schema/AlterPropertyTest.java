package com.orientechnologies.orient.server.distributed.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class AlterPropertyTest extends AbstractServerClusterTest {

  private OProperty property;

  @Test
  public void test() throws Exception {
    init(2);
    prepare(true);
    execute();
  }

  @Override
  protected void onAfterDatabaseCreation(ODatabaseDocument db) {
    OClass oClass = db.getMetadata().getSchema().createClass("AlterPropertyTestClass");

    property = oClass.createProperty("property", OType.STRING);
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
      testAlterCustomAttributeInProperty();
      testAlterCustomAttributeWithDotInProperty();
    } finally {
      db.close();
    }
  }

  private void testAlterCustomAttributeInProperty() {
    property.setCustom("customAttribute", "value");
    assertEquals("value", property.getCustom("customAttribute"));
  }

  private void testAlterCustomAttributeWithDotInProperty() {
    property.setCustom("custom.attribute", "value");
    assertEquals("value", property.getCustom("custom.attribute"));
  }
}
