package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 09.07.13
 */
public class OStoragePaginatedClusterConfiguration implements OStorageClusterConfiguration {
  public static float                    DEFAULT_GROW_FACTOR      = (float) 1.2;

  public transient OStorageConfiguration root;

  public int                             id;
  public String                          name;
  public String                          location;

  public boolean                         useWal                   = true;
  public float                           recordOverflowGrowFactor = DEFAULT_GROW_FACTOR;
  public float                           recordGrowFactor         = DEFAULT_GROW_FACTOR;
  public String                          compression              = OGlobalConfiguration.STORAGE_COMPRESSION_METHOD
                                                                      .getValueAsString();

  public OStoragePaginatedClusterConfiguration(OStorageConfiguration root, int id, String name, String location, boolean useWal,
      float recordOverflowGrowFactor, float recordGrowFactor, String compression) {
    this.root = root;
    this.id = id;
    this.name = name;
    this.location = location;
    this.useWal = useWal;
    this.recordOverflowGrowFactor = recordOverflowGrowFactor;
    this.recordGrowFactor = recordGrowFactor;
    this.compression = compression;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getLocation() {
    return location;
  }

  @Override
  public int getDataSegmentId() {
    return -1;
  }
}
