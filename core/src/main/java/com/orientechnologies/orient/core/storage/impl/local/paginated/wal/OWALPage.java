package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Basic interface for classes which present pages of WAL. Main reason of creation of this interface
 * is support of different binary formats of WAL in the same deployment.
 *
 * <p>To detect which version of binary format is stored we use value stored under {@link
 * OWALPage#MAGIC_NUMBER_OFFSET}
 */
public interface OWALPage {
  /** Size of the current instance of page in direct memory. */
  int PAGE_SIZE = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  /**
   * Size of the record which will be stored inside of page even if payload of record equals to 0.
   * That happens because when we store record inside of page we add additional system metadata are
   * used when we read record back from WAL.
   */
  int MIN_RECORD_SIZE = OIntegerSerializer.INT_SIZE + 3;

  /** Offset of position which stores CRC32 value of content stored on this page. */
  int CRC_OFFSET = 0;

  /**
   * Offset of magic number value. Randomly generated constant which is used to identify whether
   * page is broken on disk and version of binary format is used to store page.
   */
  int MAGIC_NUMBER_OFFSET = CRC_OFFSET + OIntegerSerializer.INT_SIZE;

  /** Offset of value which contains amount of space which is available to store new records. */
  int FREE_SPACE_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  /** Returns content of record which is stored inside of specified position of page. */
  byte[] getRecord(int position);

  /**
   * Indicates whether page stored inside of passed in position is stored only partially inside of
   * given page, so next part of the record should be read from next page of WAL segment.
   */
  boolean mergeWithNextPage(int position);

  /** @return Amount of free space available to store new records inside of page. */
  int getFreeSpace();
}
