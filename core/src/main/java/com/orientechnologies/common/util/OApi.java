package com.orientechnologies.common.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotation to mark OrientDB API. Maturity has the following meaning:
 *
 * <ul>
 *   <li>EXPERIMENTAL: the API can change or could be completely dropped
 *   <li>NEW: the API is new and could be immature
 *   <li>STABLE: the API is stable and used by users. Any change to the API pass for @Deprecated and
 *       official announcements
 *   <li>DEPRECATED: the API has been deprecated. Usually a better alternative is provided in
 *       JavaDoc. The Deprecated API could be removed on further releases
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@Retention(RetentionPolicy.SOURCE)
public @interface OApi {
  enum MATURITY {
    EXPERIMENTAL,
    NEW,
    STABLE,
    DEPRECATED
  }

  boolean enduser() default true;

  MATURITY maturity() default MATURITY.NEW;
}
