package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public class OPropertySBTreeRidBagIndexDefinitionTest
    extends OPropertyRidBagAbstractIndexDefinition {
  private OrientDB context;
  protected ODatabaseDocument database;
  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    String dbName = this.getClass().getSimpleName();
    context =
        new OrientDB("embedded:" + buildDirectory + "/test-db/", OrientDBConfig.defaultConfig());
    if (context.exists(dbName)) {
      context.drop(dbName);
    }
    context
        .execute(
            "create database "
                + dbName
                + " plocal users(admin identified by 'adminpwd' role admin)")
        .close();

    super.beforeMethod();

    topThreshold = OConfiguration.global().ridBagEmbeddedToSbtreebonsaiThreshold();
    bottomThreshold = OConfiguration.global().ridBagSbtreebonsaiToEmbeddedThreshold();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);

    database = context.open(this.getClass().getSimpleName(), "admin", "adminpwd");
  }

  @After
  public void afterMethod() {
    database.close();
    context.drop(this.getClass().getSimpleName());
    context.close();
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }
}
