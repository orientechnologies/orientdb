package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

import java.util.HashMap;
import java.util.Map;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
class OrientGraphContext {
  public ODatabaseDocumentTx rawGraph;
  public Thread              thread = Thread.currentThread();
}