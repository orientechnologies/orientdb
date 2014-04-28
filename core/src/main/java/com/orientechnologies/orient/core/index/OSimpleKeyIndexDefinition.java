package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OSimpleKeyIndexDefinition extends OAbstractIndexDefinition {
  private OType[] keyTypes;

  public OSimpleKeyIndexDefinition(final OType... keyTypes) {
    this.keyTypes = keyTypes;
  }

  public OSimpleKeyIndexDefinition() {
  }

  public List<String> getFields() {
    return Collections.emptyList();
  }

  public List<String> getFieldsToIndex() {
    return Collections.emptyList();
  }

  public String getClassName() {
    return null;
  }

  public Object createValue(final List<?> params) {
    return createValue(params != null ? params.toArray() : null);
  }

  public Object createValue(final Object... params) {
    if (params == null || params.length == 0)
      return null;

    if (keyTypes.length == 1)
      return OType.convert(params[0], keyTypes[0].getDefaultJavaType());

    final OCompositeKey compositeKey = new OCompositeKey();

    for (int i = 0; i < params.length; ++i) {
      final Comparable<?> paramValue = (Comparable<?>) OType.convert(params[i], keyTypes[i].getDefaultJavaType());

      if (paramValue == null)
        return null;
      compositeKey.addKey(paramValue);
    }

    return compositeKey;
  }

  public int getParamCount() {
    return keyTypes.length;
  }

  public OType[] getTypes() {
    return keyTypes;
  }

  @Override
  public ODocument toStream() {
    document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
    try {

      final List<String> keyTypeNames = new ArrayList<String>(keyTypes.length);

      for (final OType keyType : keyTypes)
        keyTypeNames.add(keyType.toString());

      document.field("keyTypes", keyTypeNames, OType.EMBEDDEDLIST);
      document.field("collate", collate.getName());
      document.field("nullValuesIgnored", isNullValuesIgnored());
      return document;
    } finally {
      document.setInternalStatus(ORecordElement.STATUS.LOADED);
    }
  }

  @Override
  protected void fromStream() {
    final List<String> keyTypeNames = document.field("keyTypes");
    keyTypes = new OType[keyTypeNames.size()];

    int i = 0;
    for (final String keyTypeName : keyTypeNames) {
      keyTypes[i] = OType.valueOf(keyTypeName);
      i++;
    }

    setCollate((String) document.field("collate"));
    setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean> field("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(final ODocument iDocument) {
    throw new OIndexException("This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OSimpleKeyIndexDefinition that = (OSimpleKeyIndexDefinition) o;
    if (!Arrays.equals(keyTypes, that.keyTypes))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (keyTypes != null ? Arrays.hashCode(keyTypes) : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OSimpleKeyIndexDefinition{" + "keyTypes=" + (keyTypes == null ? null : Arrays.asList(keyTypes)) + '}';
  }

  /**
   * {@inheritDoc}
   * 
   * @param indexName
   * @param indexType
   */
  public String toCreateIndexDDL(final String indexName, final String indexType) {
    final StringBuilder ddl = new StringBuilder("create index ");
    ddl.append(indexName).append(' ').append(indexType).append(' ');

    if (keyTypes != null && keyTypes.length > 0) {
      ddl.append(keyTypes[0].toString());
      for (int i = 1; i < keyTypes.length; i++) {
        ddl.append(", ").append(keyTypes[i].toString());
      }
    }
    return ddl.toString();
  }

  @Override
  public boolean isAutomatic() {
    return false;
  }
}
