package com.orientechnologies.orient.core.annotation;

import java.lang.annotation.*;

/**
 * Tells that the method is public and can be exposed to the client.
 *
 * @author Matan Shukry (matanshukry@gmail.com)
 * @since 3/1/2015
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface OExposedMethod {
}
