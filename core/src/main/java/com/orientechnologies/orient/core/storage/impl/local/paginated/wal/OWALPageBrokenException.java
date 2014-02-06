package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/16/13
 */
public class OWALPageBrokenException extends OException {
  public OWALPageBrokenException(String message) {
    super(message);
  }

  public OWALPageBrokenException(String message, Throwable cause) {
    super(message, cause);
  }
}
