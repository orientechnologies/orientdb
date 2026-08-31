package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import org.junit.After;
import org.junit.Before;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1/30/14
 */
public class OPropertyEmbeddedRidBagIndexDefinitionTest
    extends OPropertyRidBagAbstractIndexDefinition {
  private int topThreshold;
  private int bottomThreshold;

  @Before
  @Override
  public void beforeMethod() {
    super.beforeMethod();

    topThreshold = OConfiguration.global().ridBagEmbeddedToSbtreebonsaiThreshold();
    bottomThreshold = OConfiguration.global().ridBagSbtreebonsaiToEmbeddedThreshold();

    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(Integer.MAX_VALUE);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(Integer.MAX_VALUE);
  }

  @After
  public void afterMethod() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }
}
