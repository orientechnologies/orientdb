package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.exception.OException;

/**
 * Exception raised during not managed exceptions during update of graph.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OrientGraphModificationException extends OException {

  public OrientGraphModificationException(OException exception) {
    super(exception);
  }

  public OrientGraphModificationException(String message) {
    super(message);
  }
}
