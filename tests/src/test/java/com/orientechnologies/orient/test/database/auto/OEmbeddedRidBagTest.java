package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.remote.OrientDBRemote;
import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class OEmbeddedRidBagTest extends ORidBagTest {
  private int topThreshold;
  private int bottomThreshold;

  @Parameters(value = "url")
  public OEmbeddedRidBagTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();
    super.beforeClass();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    topThreshold = OConfiguration.global().ridBagEmbeddedToSbtreebonsaiThreshold();
    bottomThreshold = OConfiguration.global().ridBagSbtreebonsaiToEmbeddedThreshold();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);

    if (database.isRemote()) {
      OrientDBRemote internal = (OrientDBRemote) OrientDBInternal.extract(baseContext);
      internal.setGlobalConfiguration(
          "root",
          "root",
          OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD,
          String.valueOf(Integer.MAX_VALUE));
      internal.setGlobalConfiguration(
          "root",
          "root",
          OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD,
          String.valueOf(Integer.MAX_VALUE));
    }
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.isRemote()) {
      OrientDBRemote internal = (OrientDBRemote) OrientDBInternal.extract(baseContext);
      internal.setGlobalConfiguration(
          "root",
          "root",
          OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD,
          String.valueOf(topThreshold));
      internal.setGlobalConfiguration(
          "root",
          "root",
          OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD,
          String.valueOf(bottomThreshold));
    }
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue(isEmbedded);
  }
}
