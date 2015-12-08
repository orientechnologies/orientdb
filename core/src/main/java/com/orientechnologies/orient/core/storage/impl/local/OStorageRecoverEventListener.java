package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Event Listener interface invoked during storage recovering.
 * 
 * @author Luca Garulli
 */
public interface OStorageRecoverEventListener {
  void onScannedEdge(ODocument edge);

  void onRemovedEdge(ODocument edge);

  void onScannedVertex(ODocument vertex);

  void onScannedLink(OIdentifiable link);

  void onRemovedLink(OIdentifiable link);

  void onRepairedVertex(ODocument vertex);
}
