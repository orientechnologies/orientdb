package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index definition for index which is bound to field with type {@link OType#LINKBAG} .
 * 
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 1/30/14
 */
public class OPropertyRidBagIndexDefinition extends OAbstractIndexDefinitionMultiValue implements OIndexDefinitionMultiValue {
  public OPropertyRidBagIndexDefinition() {
  }

  public OPropertyRidBagIndexDefinition(String className, String field) {
    super(className, field, OType.LINK);
  }

  @Override
  public Object createSingleValue(Object... param) {
    return OType.convert(param[0], keyType.getDefaultJavaType());
  }

  public void processChangeEvent(final OMultiValueChangeEvent<?, ?> changeEvent, final Map<Object, Integer> keysToAdd,
      final Map<Object, Integer> keysToRemove) {
    switch (changeEvent.getChangeType()) {
    case ADD: {
      processAdd(createSingleValue(changeEvent.getValue()), keysToAdd, keysToRemove);
      break;
    }
    case REMOVE: {
      processRemoval(createSingleValue(changeEvent.getOldValue()), keysToAdd, keysToRemove);
      break;
    }
    default:
      throw new IllegalArgumentException("Invalid change type : " + changeEvent.getChangeType());
    }
  }

  @Override
  public Object getDocumentValueToIndex(ODocument iDocument) {
    return createValue(iDocument.field(field));
  }

  @Override
  public Object createValue(final List<?> params) {
    if (!(params.get(0) instanceof ORidBag))
      return null;

    final ORidBag ridBag = (ORidBag) params.get(0);
    final List<Object> values = new ArrayList<Object>();
    for (final OIdentifiable item : ridBag) {
      values.add(createSingleValue(item));
    }

    return values;
  }

  @Override
  public Object createValue(final Object... params) {
    if (!(params[0] instanceof ORidBag))
      return null;

    final ORidBag ridBag = (ORidBag) params[0];
    final List<Object> values = new ArrayList<Object>();
    for (final OIdentifiable item : ridBag) {
      values.add(createSingleValue(item));
    }

    return values;
  }

  @Override
  public String toCreateIndexDDL(String indexName, String indexType) {
    return createIndexDDLWithoutFieldType(indexName, indexType).toString();
  }
}