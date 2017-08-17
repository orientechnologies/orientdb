package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;

/**
 * Created by tglman on 28/07/17.
 */
public final class OLinkConverter implements OValuesConverter<OIdentifiable> {
  private OConverterData converterData;

  public OLinkConverter(OConverterData importer) {
    this.converterData = importer;
  }

  @Override
  public OIdentifiable convert(OIdentifiable value) {
    final ORID rid = value.getIdentity();
    if (!rid.isPersistent())
      return value;

    if (converterData.brokenRids.contains(rid))
      return OImportConvertersFactory.BROKEN_LINK;

    final OIdentifiable newRid = converterData.exportImportHashTable.get(rid);
    if (newRid == null)
      return value;

    return newRid.getIdentity();
  }
}
