package com.orientechnologies.website.annotation;

import java.lang.annotation.*;

/**
 * Annotation that indicates an operation should be retried if the specified exception is encountered.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface RetryingTransaction {

  /**
   * Specify exception for which operation should be retried.
   */
  Class exception() default Exception.class;

  /**
   * Sets the number of times to retry the operation. The default of -1 indicates we want to use whatever the global default is.
   */
  int retries() default -1;
}
