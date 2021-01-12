package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;

public final class CASWALPage {
  public static final long MAGIC_NUMBER = 0xEF31BCDAFL;
  public static final long MAGIC_NUMBER_WITH_ENCRYPTION = 0xEF42BCAFEL;

  /**
   * Offset of magic number value. Randomly generated constant which is used to identify whether
   * page is broken on disk and version of binary format is used to store page.
   */
  public static final int MAGIC_NUMBER_OFFSET = 0;

  /** Offset of position which stores XX_HASH value of content stored on this page. */
  public static final int XX_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  /** Offset of position which stores operation id of the last record in the page */
  public static final int LAST_OPERATION_ID_OFFSET = XX_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int PAGE_SIZE_OFFSET = LAST_OPERATION_ID_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final int DEFAULT_PAGE_SIZE = 4 * 1024;

  public static final int RECORDS_OFFSET = PAGE_SIZE_OFFSET + OShortSerializer.SHORT_SIZE;

  public static final int DEFAULT_MAX_RECORD_SIZE = DEFAULT_PAGE_SIZE - RECORDS_OFFSET;

  /**
   * Calculates how much space record will consume once it will be stored inside of page. Sizes are
   * different because once record is stored inside of the page, it is wrapped by additional system
   * information.
   */
  public static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE;
  }
}
