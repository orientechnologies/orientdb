package com.orientechnologies.orient.server.network.protocol.binary;


/**
 * Synchronous command result manager.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSyncCommandResultListener extends OAbstractCommandResultListener {

  @Override
  public boolean result(final Object iRecord) {
    fetchRecord(iRecord);
    return true;
  }

  public boolean isEmpty() {
    return false;
  }
}