package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

public class OSchemaNotCreatedException extends OSchemaException implements OHighLevelException {
  public OSchemaNotCreatedException(String message) {
    super(message);
  }

  /**
   * This constructor is needed to restore and reproduce exception on client side in case of remote
   * storage exception handling. Please create "copy constructor" for each exception which has
   * current one as a parent.
   *
   * @param exception
   */
  public OSchemaNotCreatedException(OSchemaNotCreatedException exception) {
    super(exception);
  }
}
