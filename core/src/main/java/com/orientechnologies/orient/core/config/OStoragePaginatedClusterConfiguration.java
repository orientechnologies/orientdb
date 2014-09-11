package com.orientechnologies.orient.core.config;

/**
 * @author Andrey Lomakin
 * @since 09.07.13
 */
public class OStoragePaginatedClusterConfiguration implements OStorageClusterConfiguration {
  public static float                    DEFAULT_GROW_FACTOR      = (float) 1.2;
  public float                           recordOverflowGrowFactor = DEFAULT_GROW_FACTOR;
  public float                           recordGrowFactor         = DEFAULT_GROW_FACTOR;
  public String                          compression;
  public transient OStorageConfiguration root;
  public int                             id;
  public String                          name;
  public String                          location;
  public boolean                         useWal                   = true;
  public String                          conflictStrategy;

  public OStoragePaginatedClusterConfiguration(final OStorageConfiguration root, final int id, final String name,
      final String location, final boolean useWal, final float recordOverflowGrowFactor, final float recordGrowFactor,
      final String compression, final String conflictStrategy) {
    this.root = root;
    this.id = id;
    this.name = name;
    this.location = location;
    this.useWal = useWal;
    this.recordOverflowGrowFactor = recordOverflowGrowFactor;
    this.recordGrowFactor = recordGrowFactor;
    this.compression = compression;
    this.conflictStrategy = conflictStrategy;
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
