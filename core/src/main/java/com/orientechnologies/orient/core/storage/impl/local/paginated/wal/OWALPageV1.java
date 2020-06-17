package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import java.nio.ByteBuffer;

/**
 * WAL page is organized using following format:
 *
 * <p>
 *
 * <ol>
 *   <li>CRC32 code of page content, it is used to check whether data are broken on disk. 4 bytes
 *   <li>Magic number, randomly generated number which is used to check whether page is broken on
 *       disk. 8 bytes
 *   <li>Amount of free space left on page, which can be used to add new records, 4 bytes
 *   <li>WAL records
 * </ol>
 *
 * <p>Each WAL record is stored using following format:
 *
 * <ol>
 *   <li>Flag which indicates that record should be merged with record which is stored on next page.
 *       That is needed if record can not be stored on one page and is split by two pages. 1 byte
 *   <li>Flag which indicates that this record is actually tail of long record parts of which are
 *       stored on other pages, 1 byte
 *   <li>Length of serialized content of WAL record. 4 bytes
 *   <li>Serialized content of the WAL record. Variable size.
 * </ol>
 *
 * <p>
 *
 * <p>Every time new record is added. Value of free space left on page is updated.
 *
 * @author Andrey Lomakin
 * @since 5/8/13
 */
public class OWALPageV1 implements OWALPage {
  /**
   * Value of magic number for v1 version of binary format
   *
   * @see OWALPage#MAGIC_NUMBER_OFFSET
   */
  static final long MAGIC_NUMBER = 0xFACB03FEL;

  /**
   * Offset inside of the page starting from which we will store new records till the end of the
   * page.
   */
  static final int RECORDS_OFFSET = FREE_SPACE_OFFSET + OIntegerSerializer.INT_SIZE;

  /**
   * Maximum size of the records which can be stored inside of the page
   *
   * @see #calculateRecordSize(int)
   * @see #calculateSerializedSize(int)
   */
  static final int MAX_ENTRY_SIZE = PAGE_SIZE - RECORDS_OFFSET;

  private final ByteBuffer buffer;

  OWALPageV1(ByteBuffer buffer, boolean isNew) {
    this.buffer = buffer;

    if (isNew) {
      buffer.position(MAGIC_NUMBER_OFFSET);

      buffer.putLong(MAGIC_NUMBER);
      buffer.putInt(MAX_ENTRY_SIZE);
    }
  }

  /** {@inheritDoc} */
  @Override
  public byte[] getRecord(int position) {
    buffer.position(position + 2);
    final int recordSize = buffer.getInt();
    final byte[] record = new byte[recordSize];
    buffer.get(record);
    return record;
  }

  /** {@inheritDoc} */
  @Override
  public boolean mergeWithNextPage(int position) {
    return buffer.get(position) > 0;
  }

  /** {@inheritDoc} */
  @Override
  public int getFreeSpace() {
    return buffer.getInt(FREE_SPACE_OFFSET);
  }

  /**
   * Calculates how much space record will consume once it will be stored inside of page. Sizes are
   * different because once record is stored inside of the page, it is wrapped by additional system
   * information.
   */
  static int calculateSerializedSize(int recordSize) {
    return recordSize + OIntegerSerializer.INT_SIZE + 2;
  }

  /**
   * Calculates how much space record stored inside of page will consume once it will be read from
   * page. In other words it calculates initial size of the record before it was stored inside of
   * the page. Sizes are different because once record is stored inside of the page, it is wrapped
   * by additional system information.
   */
  static int calculateRecordSize(int serializedSize) {
    return serializedSize - OIntegerSerializer.INT_SIZE - 2;
  }
}
