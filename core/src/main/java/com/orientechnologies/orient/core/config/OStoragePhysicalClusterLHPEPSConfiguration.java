package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 03.08.12
 */
public class OStoragePhysicalClusterLHPEPSConfiguration extends OStorageSegmentConfiguration implements
    OStoragePhysicalClusterConfiguration {

  private static final String       START_SIZE = "7Mb";

  private int                       dataSegmentId;
  private OStorageFileConfiguration overflowFile;
  private OStorageFileConfiguration overflowStatisticsFile;

  public OStoragePhysicalClusterLHPEPSConfiguration(OStorageConfiguration iRoot, int iId, int dataSegmentId) {
    super(iRoot, null, iId);
    this.dataSegmentId = dataSegmentId;
    fileStartSize = START_SIZE;
  }

  public OStoragePhysicalClusterLHPEPSConfiguration(final OStorageConfiguration iStorageConfiguration, final int iId,
      final int iDataSegmentId, final String iSegmentName) {
    super(iStorageConfiguration, iSegmentName, iId);
    this.dataSegmentId = iDataSegmentId;
    fileStartSize = START_SIZE;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public int getDataSegmentId() {
    return dataSegmentId;
  }

  public OStorageFileConfiguration[] getInfoFiles() {
    return infoFiles;
  }

  public String getMaxSize() {
    return maxSize;
  }

  public void setDataSegmentId(int dataSegmentId) {
    this.dataSegmentId = dataSegmentId;
  }

  public OStorageFileConfiguration getOverflowFile() {
    return overflowFile;
  }

  public void setOverflowFile(OStorageFileConfiguration overflowFile) {
    this.overflowFile = overflowFile;
  }

  public OStorageFileConfiguration getOverflowStatisticsFile() {
    return overflowStatisticsFile;
  }

  public void setOverflowStatisticsFile(OStorageFileConfiguration overflowStatisticsFile) {
    this.overflowStatisticsFile = overflowStatisticsFile;
  }

  @Override
  public void setRoot(final OStorageConfiguration root) {
    super.setRoot(root);
    overflowFile.parent = this;
    overflowStatisticsFile.parent = this;
  }
}
