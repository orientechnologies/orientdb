package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OHighLevelException;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <lomakin.andrey@gmail.com>.
 * @since 10/5/2015
 */
public class OBackupInProgressException extends OCoreException implements OHighLevelException {
  public OBackupInProgressException(OBackupInProgressException exception) {
    super(exception);
  }

  public OBackupInProgressException(String message, String componentName, OErrorCode errorCode) {
    super(message, componentName, errorCode);
  }
}
