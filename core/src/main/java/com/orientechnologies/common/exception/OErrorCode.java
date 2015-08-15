package com.orientechnologies.common.exception;

import com.orientechnologies.orient.core.exception.OQueryParsingException;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Luigi Dell'Aquila
 */
public enum OErrorCode {

  // eg.
  QUERY_PARSE_ERROR(OErrorCategory.SQL_PARSING, 1, "query parse error", OQueryParsingException.class);

  protected final OErrorCategory              category;
  protected final int                         code;
  protected final String                      description;
  protected final Class<? extends OException> exceptionClass;

  OErrorCode(OErrorCategory category, int code, String description) {
    this(category, code, description, OException.class);
  }

  OErrorCode(OErrorCategory category, int code, String description, Class<? extends OException> exceptionClass) {
    this.category = category;
    this.code = code;
    this.description = description;
    this.exceptionClass = exceptionClass;
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
    String fullMessage = String.format("%1$06d_%2$06d - %3", category.code, code, message);
    try {
      throw exceptionClass.getConstructor(String.class, Exception.class).newInstance(fullMessage, parent);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    }

  }

}
