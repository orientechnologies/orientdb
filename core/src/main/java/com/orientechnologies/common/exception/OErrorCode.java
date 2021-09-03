package com.orientechnologies.common.exception;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OApi;
import com.orientechnologies.orient.core.exception.OBackupInProgressException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import java.lang.reflect.InvocationTargetException;

/**
 * Enumeration with the error managed by OrientDB. This class has been introduced in v.2.2 and
 * little by little will contain all the OrientDB managed errors.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
@OApi(maturity = OApi.MATURITY.NEW)
public enum OErrorCode {

  // eg.
  QUERY_PARSE_ERROR(
      OErrorCategory.SQL_PARSING, 1, "query parse error", OQueryParsingException.class),

  BACKUP_IN_PROGRESS(
      OErrorCategory.STORAGE,
      2,
      "You are trying to start a backup, but it is already in progress",
      OBackupInProgressException.class),

  MVCC_ERROR(
      OErrorCategory.CONCURRENCY_RETRY,
      3,
      "The version of the update is outdated compared to the persistent value, retry",
      OConcurrentModificationException.class),

  VALIDATION_ERROR(OErrorCategory.VALIDATION, 4, "Record validation failure", OException.class),

  GENERIC_ERROR(OErrorCategory.SQL_GENERIC, 5, "Generic Error", OException.class);

  private static OErrorCode[] codes = new OErrorCode[6];

  static {
    for (OErrorCode code : OErrorCode.values()) {
      codes[code.getCode()] = code;
    }
  }

  protected final OErrorCategory category;
  protected final int code;
  protected final String description;
  protected final Class<? extends OException> exceptionClass;

  OErrorCode(
      OErrorCategory category,
      int code,
      String description,
      Class<? extends OException> exceptionClass) {
    this.category = category;
    this.code = code;
    this.description = description;
    this.exceptionClass = exceptionClass;
  }

  public int getCode() {
    return code;
  }

  public void throwException() {
    throwException(this.description, null);
  }

  public void throwException(String message) {
    throwException(message, null);
  }

  public void throwException(Throwable parent) {
    throwException(this.description, parent);
  }

  public void throwException(String message, Throwable parent) {
    OException exc = newException(message, parent);
    throw exc;
  }

  public OException newException(String message, Throwable parent) {
    final String fullMessage = String.format("%1$06d_%2$06d - %3$s", category.code, code, message);
    try {
      return OException.wrapException(
          exceptionClass.getConstructor(String.class).newInstance(fullMessage), parent);
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      OLogManager.instance().warn(this, "Cannot instantiate exception " + exceptionClass);
    }
    return null;
  }

  public static OErrorCode getErrorCode(int code) {
    return codes[code];
  }
}
