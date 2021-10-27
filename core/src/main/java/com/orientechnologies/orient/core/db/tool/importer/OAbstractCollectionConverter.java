package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/** Created by tglman on 28/07/17. */
public abstract class OAbstractCollectionConverter<T> implements OValuesConverter<T> {
  private final OConverterData converterData;

  protected OAbstractCollectionConverter(OConverterData converterData) {
    this.converterData = converterData;
  }

  public interface ResultCallback {
    void add(Object item);
  }

  protected boolean convertSingleValue(final Object item, ResultCallback result, boolean updated) {
    if (item == null) {
      result.add(null);
      return false;
    }

    if (item instanceof OIdentifiable) {
      final OValuesConverter<OIdentifiable> converter =
          (OValuesConverter<OIdentifiable>)
              OImportConvertersFactory.INSTANCE.getConverter(item, converterData);

      final OIdentifiable newValue = converter.convert((OIdentifiable) item);

      // this code intentionally uses == instead of equals, in such case we may distinguish rids
      // which already contained in
      // document and RID which is used to indicate broken record
      if (newValue != OImportConvertersFactory.BROKEN_LINK) result.add(newValue);

      if (!newValue.equals(item)) updated = true;
    } else {
      final OValuesConverter valuesConverter =
          OImportConvertersFactory.INSTANCE.getConverter(item, converterData);
      if (valuesConverter == null) result.add(item);
      else {
        final Object newValue = valuesConverter.convert(item);
        if (newValue != item) updated = true;

        result.add(newValue);
      }
    }

    return updated;
  }
}
