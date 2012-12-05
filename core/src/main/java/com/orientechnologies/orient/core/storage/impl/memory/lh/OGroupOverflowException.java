package com.orientechnologies.orient.core.storage.impl.memory.lh;

import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * @author Artem Loginov (artem.loginov@exigenservices.com)
 */
public class OGroupOverflowException extends ODatabaseException {
  public OGroupOverflowException(String s) {
    super(s);
  }
}
