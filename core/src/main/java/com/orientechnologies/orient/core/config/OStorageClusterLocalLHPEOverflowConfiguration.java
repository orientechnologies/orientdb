package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 03.08.12
 */
public class OStorageClusterLocalLHPEOverflowConfiguration extends OStorageFileConfiguration {
  private static final long   serialVersionUID   = 1L;

  private static final String DEF_EXTENSION      = ".oco";
  private static final String DEF_INCREMENT_SIZE = "50%";

  public OStorageClusterLocalLHPEOverflowConfiguration() {
  }

  public OStorageClusterLocalLHPEOverflowConfiguration(OStorageSegmentConfiguration iParent, String iPath, String iType,
      String iMaxSize) {
    super(iParent, iPath + DEF_EXTENSION, iType, iMaxSize, DEF_INCREMENT_SIZE);
  }
}
