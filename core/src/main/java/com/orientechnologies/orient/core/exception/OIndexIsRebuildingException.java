package com.orientechnologies.orient.core.exception;

/**
 * Exception which is thrown by {@link com.orientechnologies.orient.core.index.OIndexChangesWrapper}
 * if index which is related to wrapped cursor is being rebuilt.
 *
 * @see com.orientechnologies.orient.core.index.OIndexAbstract#getRebuildVersion()
 */
public class OIndexIsRebuildingException extends ORetryQueryException {
  public OIndexIsRebuildingException(OIndexIsRebuildingException exception) {
    super(exception);
  }

  public OIndexIsRebuildingException(String message) {
    super(message);
  }
}
