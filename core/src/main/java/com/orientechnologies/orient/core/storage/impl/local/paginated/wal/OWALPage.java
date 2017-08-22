package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
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
  int PAGE_SIZE       = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;
  int MIN_RECORD_SIZE = OIntegerSerializer.INT_SIZE + 3;

  int CRC_OFFSET          = 0;
  int MAGIC_NUMBER_OFFSET = CRC_OFFSET + OIntegerSerializer.INT_SIZE;
  int FREE_SPACE_OFFSET   = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  byte[] getRecord(int position);

  boolean mergeWithNextPage(int position);

  int getFreeSpace();
}
