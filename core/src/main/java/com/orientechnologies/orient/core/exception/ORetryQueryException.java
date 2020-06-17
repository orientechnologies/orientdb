package com.orientechnologies.orient.core.exception;

/**
 * Exception which is thrown by core components to ask command handler to rebuild and run executed
 * command again.
 *
 * @see com.orientechnologies.orient.core.index.OIndexAbstract#getRebuildVersion()
 */
public abstract class ORetryQueryException extends OCoreException {
  public ORetryQueryException(ORetryQueryException exception) {
    super(exception);
  }

  public ORetryQueryException(String message) {
    super(message);
  }
}
