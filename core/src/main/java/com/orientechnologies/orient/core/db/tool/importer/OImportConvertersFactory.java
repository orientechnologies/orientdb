package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Created by tglman on 28/07/17. */
public final class OImportConvertersFactory {
  public static final ORID BROKEN_LINK = new ORecordId(-1, -42);
  public static final OImportConvertersFactory INSTANCE = new OImportConvertersFactory();

  public OValuesConverter getConverter(Object value, OConverterData converterData) {
    if (value instanceof Map) return new OMapConverter(converterData);

    if (value instanceof List) return new OListConverter(converterData);

    if (value instanceof Set) return new OSetConverter(converterData);

    if (value instanceof ORidBag) return new ORidBagConverter(converterData);

    if (value instanceof OIdentifiable) return new OLinkConverter(converterData);

    return null;
  }
}
