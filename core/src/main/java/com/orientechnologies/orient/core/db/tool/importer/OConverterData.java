package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;

import java.util.Set;

/**
 * Created by tglman on 28/07/17.
 */
public class OConverterData {
  protected OIndex<OIdentifiable> exportImportHashTable;
  protected Set<ORID>             brokenRids;

  public OConverterData(OIndex<OIdentifiable> exportImportHashTable, Set<ORID> brokenRids) {
    this.exportImportHashTable = exportImportHashTable;
    this.brokenRids = brokenRids;
  }
}
