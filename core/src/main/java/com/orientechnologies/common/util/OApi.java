package com.orientechnologies.common.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to mark OrientDB API.
 * 
 * @author Luca Garulli
 */
@Retention(RetentionPolicy.SOURCE)
public @interface OApi {
  public enum MATURITY {
    EXPERIMENTAL, NEW, STABLE, DEPRECATED
  }

  boolean enduser() default true;

  MATURITY maturity() default MATURITY.NEW;
}
