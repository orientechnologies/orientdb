package com.orientechnologies.orient.core.index;

import java.util.List;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Contains helper methods for {@link OIndexDefinition} creation.
 * 
 * <b>IMPORTANT:</b> This class designed for internal usage only.
 * 
 * @author Artem Orobets
 */
public class OIndexDefinitionFactory {
  /**
   * Creates an instance of {@link OIndexDefinition} for automatic index.
   * 
   * @param oClass
   *          class which will be indexed
   * @param fieldNames
   *          list of properties which will be indexed. Format should be '<property> [by key|value]', use 'by key' or 'by value' to
   *          describe how to index maps. By default maps indexed by key
   * @param types
   *          types of indexed properties
   * @return index definition instance
   */
  public static OIndexDefinition createIndexDefinition(final OClass oClass, final List<String> fieldNames, final List<OType> types) {
    checkTypes(oClass, fieldNames, types);

    if (fieldNames.size() == 1)
      return createSingleFieldIndexDefinition(oClass, fieldNames.get(0), types.get(0));
    else
      return createMultipleFieldIndexDefinition(oClass, fieldNames, types);
  }

  /**
   * Extract field name from '<property> [by key|value]' field format.
   * 
   * @param fieldDefinition
   *          definition of field
   * @return extracted property name
   */
  public static String extractFieldName(final String fieldDefinition) {
    String[] fieldNameParts = fieldDefinition.split("\\s+");
    if (fieldNameParts.length == 1)
      return fieldDefinition;
    if (fieldNameParts.length == 3 && "by".equalsIgnoreCase(fieldNameParts[1]))
      return fieldNameParts[0];

    throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '"
        + fieldDefinition + "'");
  }

  private static OIndexDefinition createMultipleFieldIndexDefinition(final OClass oClass, final List<String> fieldsToIndex,
      final List<OType> types) {
    final OIndexDefinition indexDefinition;
    final String className = oClass.getName();
    final OCompositeIndexDefinition compositeIndex = new OCompositeIndexDefinition(className);

    for (int i = 0, fieldsToIndexSize = fieldsToIndex.size(); i < fieldsToIndexSize; i++) {
      String fieldName = adjustFieldName(oClass, fieldsToIndex.get(i));
      final OType propertyType = types.get(i);
      if (propertyType.equals(OType.EMBEDDEDLIST) || propertyType.equals(OType.EMBEDDEDSET) || propertyType.equals(OType.LINKSET)
          || propertyType.equals(OType.LINKLIST) || propertyType.equals(OType.EMBEDDEDMAP) || propertyType.equals(OType.LINKMAP))
        throw new OIndexException("Collections are not supported in composite indexes");

      final OPropertyIndexDefinition propertyIndex = new OPropertyIndexDefinition(className, fieldName, propertyType);
      compositeIndex.addIndex(propertyIndex);
    }

    indexDefinition = compositeIndex;
    return indexDefinition;
  }

  private static void checkTypes(OClass oClass, List<String> fieldNames, List<OType> types) {
    if (fieldNames.size() != types.size())
      throw new IllegalArgumentException("Count of field names doesn't match count of field types. It was " + fieldNames.size()
          + " fields, but " + types.size() + " types.");

    for (int i = 0, fieldNamesSize = fieldNames.size(); i < fieldNamesSize; i++) {
      String fieldName = fieldNames.get(i);
      OType type = types.get(i);

      final OProperty property = oClass.getProperty(fieldName);
      if (property != null && !type.equals(property.getType())) {
        throw new IllegalArgumentException("Property type list not match with real property types");
      }
    }
  }

  private static OIndexDefinition createSingleFieldIndexDefinition(OClass oClass, final String field, final OType type) {
    final String fieldName = adjustFieldName(oClass, extractFieldName(field));
    final OIndexDefinition indexDefinition;

    final OType indexType;
    if (type == OType.EMBEDDEDMAP || type == OType.LINKMAP) {
      final OPropertyMapIndexDefinition.INDEX_BY indexBy = extractMapIndexSpecifier(field);

      if (indexBy.equals(OPropertyMapIndexDefinition.INDEX_BY.KEY))
        indexType = OType.STRING;
      else {
        if (type == OType.LINKMAP)
          indexType = OType.LINK;
        else {
          final OProperty propertyToIndex = oClass.getProperty(fieldName);
          indexType = propertyToIndex.getLinkedType();
          if (indexType == null)
            throw new OIndexException("Linked type was not provided."
                + " You should provide linked type for embedded collections that are going to be indexed.");
        }

      }

      indexDefinition = new OPropertyMapIndexDefinition(oClass.getName(), fieldName, indexType, indexBy);
    } else if (type.equals(OType.EMBEDDEDLIST) || type.equals(OType.EMBEDDEDSET) || type.equals(OType.LINKLIST)
        || type.equals(OType.LINKSET)) {
      if (type.equals(OType.LINKSET))
        throw new OIndexException("LINKSET indexing is not supported.");
      else if (type.equals(OType.LINKLIST)) {
        indexType = OType.LINK;
      } else {
        final OProperty propertyToIndex = oClass.getProperty(fieldName);
        indexType = propertyToIndex.getLinkedType();
        if (indexType == null)
          throw new OIndexException("Linked type was not provided."
              + " You should provide linked type for embedded collections that are going to be indexed.");
      }

      indexDefinition = new OPropertyListIndexDefinition(oClass.getName(), fieldName, indexType);
    } else
      indexDefinition = new OPropertyIndexDefinition(oClass.getName(), fieldName, type);
    return indexDefinition;
  }

  private static OPropertyMapIndexDefinition.INDEX_BY extractMapIndexSpecifier(final String fieldName) {
    String[] fieldNameParts = fieldName.split("\\s+");
    if (fieldNameParts.length == 1)
      return OPropertyMapIndexDefinition.INDEX_BY.KEY;

    if (fieldNameParts.length == 3) {
      if ("by".equals(fieldNameParts[1].toLowerCase()))
        try {
          return OPropertyMapIndexDefinition.INDEX_BY.valueOf(fieldNameParts[2].toUpperCase());
        } catch (IllegalArgumentException iae) {
          throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '"
              + fieldName + "'");
        }
    }

    throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName
        + "'");
  }

  private static String adjustFieldName(final OClass clazz, final String fieldName) {
    final OProperty property = clazz.getProperty(fieldName);

    if (property != null)
      return property.getName();
    else
      return fieldName;
  }
}
