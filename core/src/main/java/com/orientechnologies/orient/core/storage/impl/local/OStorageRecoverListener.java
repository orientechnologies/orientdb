package com.orientechnologies.orient.core.storage.impl.local;

/**
 * Allows listeners to be notified in case of recovering is started at storage open.
 * 
 * @author Luca Garulli
 */
public interface OStorageRecoverListener {
  void onStorageRecover();
}
