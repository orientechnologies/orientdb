package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.index.OIndexAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

/**
 * Exception which is thrown by core components to ask command handler
 * {@link OAbstractPaginatedStorage#command(com.orientechnologies.orient.core.command.OCommandRequestText)}
 * to rebuild and run executed command again.
 *
 * @see OIndexAbstract#getRebuildVersion()
 */
public abstract class ORetryQueryException extends OException {
  public ORetryQueryException(String message) {
    super(message);
  }
}
