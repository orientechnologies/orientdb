package com.orientechnologies.common.exception;

/**
 * Created by luigidellaquila on 13/08/15.
 */
public enum OErrorCategory {

  SQL_GENERIC(1),

  SQL_PARSING(2),

  STORAGE(3);

  protected final int code;

  private OErrorCategory(int code) {
    this.code = code;
  }

}
