package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin
 * @since 30.05.13
 */
public interface OClusterAwareWALRecord {
  int getClusterId();
}
