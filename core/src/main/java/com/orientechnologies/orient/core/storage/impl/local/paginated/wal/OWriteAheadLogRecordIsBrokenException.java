package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public class OWriteAheadLogRecordIsBrokenException extends OException {
  public OWriteAheadLogRecordIsBrokenException(String message) {
    super(message);
  }

  public OWriteAheadLogRecordIsBrokenException(String message, final Throwable cause) {
    super(message, cause);
  }
}
