package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Basic interface for classes which present pages of WAL.
 * Main reason of creation of this interface is support of different binary formats
 * of WAL in the same deployment.
 * <p>
 * To detect which version of binary format is stored we use value stored under
 * {@link OWALPage#MAGIC_NUMBER_OFFSET}
 */
public interface OWALPage {
  /**
   * Size of the current instance of page in direct memory.
   */
  int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  /**
   * Offset of position which stores CRC32 value of content stored on this page.
   */
  int CRC_OFFSET = 0;

  /**
   * Offset of magic number value. Randomly generated constant which is used to identify whether page is broken on disk and
   * version of binary format is used to store page.
   */
  int MAGIC_NUMBER_OFFSET = CRC_OFFSET + OIntegerSerializer.INT_SIZE;

  /**
   * Returns content of record which is stored inside of specified position of page.
   */
  byte[] getRecord(int position);

  /**
   * @return Amount of free space available to store new records inside of page.
   */
  int getFreeSpace();
}
