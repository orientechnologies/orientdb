package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Set;

/** Created by tglman on 28/07/17. */
public class OConverterData {
  protected ODatabaseSession session;
  protected Set<ORID> brokenRids;

  public OConverterData(ODatabaseSession session, Set<ORID> brokenRids) {
    this.session = session;
    this.brokenRids = brokenRids;
  }
}
