package com.orientechnologies.orient.core.exception;

/**
 * Special type of exception which indicates that invalid index id was passed into
 * storage and index data should be reloaded
 */
public class OInvalidIndexEngineIdException extends Exception {
  public OInvalidIndexEngineIdException(String message) {
    super(message);
  }
}
