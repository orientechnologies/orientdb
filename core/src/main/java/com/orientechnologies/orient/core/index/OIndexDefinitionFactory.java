/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.index;

import java.util.List;
import java.util.regex.Pattern;

import com.orientechnologies.orient.core.collate.OCollate;
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
  private static final Pattern FILED_NAME_PATTERN = Pattern.compile("\\s+");

  /**
   * Creates an instance of {@link OIndexDefinition} for automatic index.
   * 
   * 
   * @param oClass
   *          class which will be indexed
   * @param fieldNames
   *          list of properties which will be indexed. Format should be '<property> [by key|value]', use 'by key' or 'by value' to
   *          describe how to index maps. By default maps indexed by key
   * @param types
   *          types of indexed properties
   * @param collates
   * @param indexKind
   * @param algorithm
   * @return index definition instance
   */
  public static OIndexDefinition createIndexDefinition(final OClass oClass, final List<String> fieldNames, final List<OType> types,
      List<OCollate> collates, String indexKind, String algorithm) {
    checkTypes(oClass, fieldNames, types);

    if (fieldNames.size() == 1)
      return createSingleFieldIndexDefinition(oClass, fieldNames.get(0), types.get(0), collates == null ? null : collates.get(0),
          indexKind, algorithm);
    else
      return createMultipleFieldIndexDefinition(oClass, fieldNames, types, collates, indexKind, algorithm);
  }

  /**
   * Extract field name from '<property> [by key|value]' field format.
   * 
   * @param fieldDefinition
   *          definition of field
   * @return extracted property name
   */
  public static String extractFieldName(final String fieldDefinition) {
    String[] fieldNameParts = FILED_NAME_PATTERN.split(fieldDefinition);
    if (fieldNameParts.length == 1)
      return fieldDefinition;
    if (fieldNameParts.length == 3 && "by".equalsIgnoreCase(fieldNameParts[1]))
      return fieldNameParts[0];

    throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '"
        + fieldDefinition + '\'');
  }

  private static OIndexDefinition createMultipleFieldIndexDefinition(final OClass oClass, final List<String> fieldsToIndex,
      final List<OType> types, List<OCollate> collates, String indexKind, String algorithm) {
    final OIndexFactory factory = OIndexes.getFactory(indexKind, algorithm);
    final String className = oClass.getName();
    final OCompositeIndexDefinition compositeIndex = new OCompositeIndexDefinition(className);

    for (int i = 0, fieldsToIndexSize = fieldsToIndex.size(); i < fieldsToIndexSize; i++) {
      OCollate collate = null;
      if (collates != null)
        collate = collates.get(i);

      compositeIndex.addIndex(createSingleFieldIndexDefinition(oClass, fieldsToIndex.get(i), types.get(i), collate, indexKind,
          algorithm));
    }

    return compositeIndex;
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

  private static OIndexDefinition createSingleFieldIndexDefinition(OClass oClass, final String field, final OType type,
      OCollate collate, String indexKind, String algorithm) {

    final OIndexFactory factory = OIndexes.getFactory(indexKind, algorithm);
    final String fieldName = adjustFieldName(oClass, extractFieldName(field));
    final OIndexDefinition indexDefinition;

    final OProperty propertyToIndex = oClass.getProperty(fieldName);
    final OType indexType;
    if (type == OType.EMBEDDEDMAP || type == OType.LINKMAP) {
      final OPropertyMapIndexDefinition.INDEX_BY indexBy = extractMapIndexSpecifier(field);

      if (indexBy.equals(OPropertyMapIndexDefinition.INDEX_BY.KEY))
        indexType = OType.STRING;
      else {
        if (type == OType.LINKMAP)
          indexType = OType.LINK;
        else {
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
        indexType = OType.LINK;
      else if (type.equals(OType.LINKLIST)) {
        indexType = OType.LINK;
      } else {
        indexType = propertyToIndex.getLinkedType();
        if (indexType == null)
          throw new OIndexException("Linked type was not provided."
              + " You should provide linked type for embedded collections that are going to be indexed.");
      }

      indexDefinition = new OPropertyListIndexDefinition(oClass.getName(), fieldName, indexType);
    } else if (type.equals(OType.LINKBAG)) {
      indexDefinition = new OPropertyRidBagIndexDefinition(oClass.getName(), fieldName);
    } else
      indexDefinition = new OPropertyIndexDefinition(oClass.getName(), fieldName, type);

    if (collate == null && propertyToIndex != null)
      collate = propertyToIndex.getCollate();

    if (collate != null)
      indexDefinition.setCollate(collate);

    return indexDefinition;
  }

  private static OPropertyMapIndexDefinition.INDEX_BY extractMapIndexSpecifier(final String fieldName) {
    String[] fieldNameParts = FILED_NAME_PATTERN.split(fieldName);
    if (fieldNameParts.length == 1)
      return OPropertyMapIndexDefinition.INDEX_BY.KEY;

    if (fieldNameParts.length == 3) {
      if ("by".equals(fieldNameParts[1].toLowerCase()))
        try {
          return OPropertyMapIndexDefinition.INDEX_BY.valueOf(fieldNameParts[2].toUpperCase());
        } catch (IllegalArgumentException iae) {
          throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '"
              + fieldName + '\'', iae);
        }
    }

    throw new IllegalArgumentException("Illegal field name format, should be '<property> [by key|value]' but was '" + fieldName
        + '\'');
  }

  private static String adjustFieldName(final OClass clazz, final String fieldName) {
    final OProperty property = clazz.getProperty(fieldName);

    if (property != null)
      return property.getName();
    else
      return fieldName;
  }
}
