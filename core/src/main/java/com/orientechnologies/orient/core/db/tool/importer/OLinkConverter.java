package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

/** Created by tglman on 28/07/17. */
public final class OLinkConverter implements OValuesConverter<OIdentifiable> {
  private OConverterData converterData;

  public OLinkConverter(OConverterData importer) {
    this.converterData = importer;
  }

  @Override
  public OIdentifiable convert(OIdentifiable value) {
    final ORID rid = value.getIdentity();
    if (!rid.isPersistent()) return value;

    if (converterData.brokenRids.contains(rid)) return OImportConvertersFactory.BROKEN_LINK;

    try (final OResultSet resultSet =
        converterData.session.query(
            "select value from " + ODatabaseImport.EXPORT_IMPORT_CLASS_NAME + " where key = ?",
            rid.toString())) {
      if (resultSet.hasNext()) {
        return new ORecordId(resultSet.next().<String>getProperty("value"));
      }
      return value;
    }
  }
}
