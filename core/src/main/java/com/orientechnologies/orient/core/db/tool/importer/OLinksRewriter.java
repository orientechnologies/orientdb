package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.document.ODocumentFieldVisitor;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.metadata.schema.OType;

/** Created by tglman on 28/07/17. */
public final class OLinksRewriter implements ODocumentFieldVisitor {
  private OConverterData converterData;

  public OLinksRewriter(OConverterData converterData) {
    this.converterData = converterData;
  }

  @Override
  public Object visitField(OType type, OType linkedType, Object value) {
    boolean oldAutoConvertValue = false;
    if (value instanceof ORecordLazyMultiValue) {
      ORecordLazyMultiValue multiValue = (ORecordLazyMultiValue) value;
      oldAutoConvertValue = multiValue.isAutoConvertToRecord();
      multiValue.setAutoConvertToRecord(false);
    }

    final OValuesConverter valuesConverter =
        OImportConvertersFactory.INSTANCE.getConverter(value, converterData);
    if (valuesConverter == null) return value;

    final Object newValue = valuesConverter.convert(value);

    if (value instanceof ORecordLazyMultiValue) {
      ORecordLazyMultiValue multiValue = (ORecordLazyMultiValue) value;
      multiValue.setAutoConvertToRecord(oldAutoConvertValue);
    }

    // this code intentionally uses == instead of equals, in such case we may distinguish rids which
    // already contained in
    // document and RID which is used to indicate broken record
    if (newValue == OImportConvertersFactory.BROKEN_LINK) return null;

    return newValue;
  }

  @Override
  public boolean goFurther(OType type, OType linkedType, Object value, Object newValue) {
    return true;
  }

  @Override
  public boolean goDeeper(OType type, OType linkedType, Object value) {
    return true;
  }

  @Override
  public boolean updateMode() {
    return true;
  }
}
