package com.orientechnologies.orient.core.db;

/** Created by tglman on 20/01/17. */
public interface OrientDBListener {

  void onCreate(ODatabase database);

  void onOpen(ODatabase database);

  void onClose(ODatabase database);

  void onDrop(ODatabase database);
}
