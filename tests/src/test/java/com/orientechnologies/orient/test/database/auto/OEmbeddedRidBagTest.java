package com.orientechnologies.orient.test.database.auto;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.OStorageProxy;

@Test
public class OEmbeddedRidBagTest extends ORidBagTest {
  private int topThreshold;
  private int bottomThreshold;

  @Parameters(value = "url")
  public OEmbeddedRidBagTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    topThreshold = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);

    if (database.getStorage() instanceof OStorageProxy) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", ODatabaseHelper.getServerRootPassword());
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, Integer.MAX_VALUE);
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, Integer.MAX_VALUE);
      server.close();
    }
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.getStorage() instanceof OStorageProxy) {
      OServerAdmin server = new OServerAdmin(database.getURL()).connect("root", ODatabaseHelper.getServerRootPassword());
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, topThreshold);
      server.setGlobalConfiguration(OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, bottomThreshold);
      server.close();
    }
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue(isEmbedded);
  }
}
