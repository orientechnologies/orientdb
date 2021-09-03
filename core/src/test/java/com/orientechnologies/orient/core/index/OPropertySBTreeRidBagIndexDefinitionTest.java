package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Before;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public class OPropertySBTreeRidBagIndexDefinitionTest
    extends OPropertyRidBagAbstractIndexDefinition {
  protected ODatabaseDocumentTx database;
  private int topThreshold;
  private int bottomThreshold;

  public OPropertySBTreeRidBagIndexDefinitionTest() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    final String url = "plocal:" + buildDirectory + "/test-db/" + this.getClass().getSimpleName();
    database = new ODatabaseDocumentTx(url);
    if (database.exists()) {
      database.open("admin", "admin");
      database.drop();
    }

    database.create();
    database.close();
  }

  @Before
  public void beforeMethod2() {
    super.beforeMethod();

    topThreshold =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    database.open("admin", "admin");
  }

  @After
  public void afterMethod() {
    database.close();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }
}
