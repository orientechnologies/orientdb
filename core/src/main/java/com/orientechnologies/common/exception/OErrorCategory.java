package com.orientechnologies.common.exception;

/** Created by luigidellaquila on 13/08/15. */
public enum OErrorCategory {
  GENERIC(1),

  SQL_GENERIC(2),

  SQL_PARSING(3),

  STORAGE(4),

  CONCURRENCY_RETRY(5),

  VALIDATION(6),

  CONCURRENCY(7);

  protected final int code;

  private OErrorCategory(int code) {
    this.code = code;
  }
}
