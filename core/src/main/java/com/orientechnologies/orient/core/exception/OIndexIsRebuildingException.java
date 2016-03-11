package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.index.OIndexAbstract;

/**
 * Exception which is thrown by {@link com.orientechnologies.orient.core.index.OIndexChangesWrapper}
 * if index which is related to wrapped cursor is being rebuilt.
 *
 * @see OIndexAbstract#getRebuildVersion()
 */
public class OIndexIsRebuildingException extends ORetryQueryException {
  public OIndexIsRebuildingException(String message) {
    super(message);
  }
}
