package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;

public final class OCASWALPage {
  static final long MAGIC_NUMBER = 0xEF31BCAFL;

  /**
   * Offset of position which stores CRC32 value of content stored on this page.
   */
  public static final int XX_OFFSET = 0;

  /**
   * Offset of magic number value. Randomly generated constant which is used to identify whether page is broken on disk and
   * version of binary format is used to store page.
   */
  public static final int MAGIC_NUMBER_OFFSET = XX_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int PAGE_SIZE_OFFSET = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int PAGE_SIZE = 4 * 1024;

  public static final int RECORDS_OFFSET = PAGE_SIZE_OFFSET + OShortSerializer.SHORT_SIZE;

  public static final int MAX_RECORD_SIZE = PAGE_SIZE - RECORDS_OFFSET;

  /**
   * Calculates how much space record will consume once it will be stored inside of page.
   * Sizes are different because once record is stored inside of the page, it is wrapped by additional system information.
   */
  static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE;
  }
}
