/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Contains helper methods for {@link OIndexDefinition} creation.
 *
 * <p><b>IMPORTANT:</b> This class designed for internal usage only.
 *
 * @author Artem Orobets
 */
public class OIndexDefinitionFactory {
  private static final Pattern FILED_NAME_PATTERN = Pattern.compile("\\s+");

  /**
   * Creates an instance of {@link OIndexDefinition} for automatic index.
   *
   * @param oClass class which will be indexed
   * @param fieldNames list of properties which will be indexed. Format should be '<property> [by
   *     key|value]', use 'by key' or 'by value' to describe how to index maps. By default maps
   *     indexed by key
   * @param types types of indexed properties
   * @param collates
   * @param indexKind
   * @param algorithm
   * @return index definition instance
   */
  public static OIndexDefinition createIndexDefinition(
      final OClass oClass,
      final List<String> fieldNames,
      final List<OType> types,
      List<OCollate> collates,
      String indexKind,
      String algorithm) {
    checkTypes(oClass, fieldNames, types);

    if (fieldNames.size() == 1) {
      OCollate collate = null;
      OType linkedType = null;
      OType type = types.get(0);
      String field = fieldNames.get(0);
      final String fieldName =
          OClassImpl.decodeClassName(adjustFieldName(oClass, extractFieldName(field)));
      if (collates != null) collate = collates.get(0);
      OProperty property = oClass.getProperty(fieldName);
      if (property != null) {
        if (collate == null) {
          collate = property.getCollate();
        }
        linkedType = property.getLinkedType();
      }

      final OPropertyMapIndexDefinition.INDEX_BY indexBy = extractMapIndexSpecifier(field);
      return createSingleFieldIndexDefinition(
          oClass.getName(), fieldName, type, linkedType, collate, indexKind, indexBy);
    } else {
      return createMultipleFieldIndexDefinition(
          oClass, fieldNames, types, collates, indexKind, algorithm);
    }
  }

  /**
   * Extract field name from '<property> [by key|value]' field format.
   *
   * @param fieldDefinition definition of field
   * @return extracted property name
   */
  public static String extractFieldName(final String fieldDefinition) {
    String[] fieldNameParts = FILED_NAME_PATTERN.split(fieldDefinition);
    if (fieldNameParts.length == 0) {
      throw new IllegalArgumentException(
          "Illegal field name format, should be '<property> [by key|value]' but was '"
              + fieldDefinition
              + '\'');
    }
    if (fieldNameParts.length == 3 && "by".equalsIgnoreCase(fieldNameParts[1]))
      return fieldNameParts[0];

    if (fieldNameParts.length == 1) return fieldDefinition;

    StringBuilder result = new StringBuilder();
    result.append(fieldNameParts[0]);
    for (int i = 1; i < fieldNameParts.length; i++) {
      result.append(" ");
      result.append(fieldNameParts[i]);
    }
    return result.toString();
  }

  private static OIndexDefinition createMultipleFieldIndexDefinition(
      final OClass oClass,
      final List<String> fieldsToIndex,
      final List<OType> types,
      List<OCollate> collates,
      String indexKind,
      String algorithm) {
    final OIndexFactory factory = OIndexes.getFactory(indexKind, algorithm);
    final String className = oClass.getName();
    final OCompositeIndexDefinition compositeIndex = new OCompositeIndexDefinition(className);

    for (int i = 0, fieldsToIndexSize = fieldsToIndex.size(); i < fieldsToIndexSize; i++) {
      OCollate collate = null;
      OType linkedType = null;
      OType type = types.get(i);
      if (collates != null) collate = collates.get(i);
      String field = fieldsToIndex.get(i);
      final String fieldName =
          OClassImpl.decodeClassName(adjustFieldName(oClass, extractFieldName(field)));
      OProperty property = oClass.getProperty(fieldName);
      if (property != null) {
        if (collate == null) {
          collate = property.getCollate();
        }
        linkedType = property.getLinkedType();
      }
      final OPropertyMapIndexDefinition.INDEX_BY indexBy = extractMapIndexSpecifier(field);

      compositeIndex.addIndex(
          createSingleFieldIndexDefinition(
              className, fieldName, type, linkedType, collate, indexKind, indexBy));
    }
    return compositeIndex;
  }

  private static void checkTypes(OClass oClass, List<String> fieldNames, List<OType> types) {
    if (fieldNames.size() != types.size()) {
      throw new IllegalArgumentException(
          "Count of field names doesn't match count of field types. It was "
              + fieldNames.size()
              + " fields, but "
              + types.size()
              + " types.");
    }

    for (int i = 0, fieldNamesSize = fieldNames.size(); i < fieldNamesSize; i++) {
      final String fieldName = fieldNames.get(i);
      final OType type = types.get(i);

      final OProperty property = oClass.getProperty(fieldName);
      if (property != null && !type.equals(property.getType())) {
        throw new IllegalArgumentException("Property type list not match with real property types");
      }
    }
  }

  public static OIndexDefinition createSingleFieldIndexDefinition(
      final String className,
      final String fieldName,
      final OType type,
      final OType linkedType,
      OCollate collate,
      final String indexKind,
      final OPropertyMapIndexDefinition.INDEX_BY indexBy) {
    // TODO: let index implementations name their preferences_
    if (type.equals(OType.EMBEDDED)) {
      if (indexKind.equals("FULLTEXT")) {
        throw new UnsupportedOperationException(
            "Fulltext index does not support embedded types: " + type);
      }
    }

    final OIndexDefinition indexDefinition;

    final OType indexType;
    if (type == OType.EMBEDDEDMAP || type == OType.LINKMAP) {

      if (indexBy == null) {
        throw new IllegalArgumentException(
            "Illegal field name format, should be '<property> [by key|value]' but was '"
                + fieldName
                + '\'');
      }
      if (indexBy.equals(OPropertyMapIndexDefinition.INDEX_BY.KEY)) indexType = OType.STRING;
      else {
        if (type == OType.LINKMAP) indexType = OType.LINK;
        else {
          indexType = linkedType;
          if (indexType == null)
            throw new OIndexException(
                "Linked type was not provided."
                    + " You should provide linked type for embedded collections that are going to be indexed.");
        }
      }
      indexDefinition = new OPropertyMapIndexDefinition(className, fieldName, indexType, indexBy);
    } else if (type.equals(OType.EMBEDDEDLIST)
        || type.equals(OType.EMBEDDEDSET)
        || type.equals(OType.LINKLIST)
        || type.equals(OType.LINKSET)) {
      if (type.equals(OType.LINKSET)) indexType = OType.LINK;
      else if (type.equals(OType.LINKLIST)) {
        indexType = OType.LINK;
      } else {
        indexType = linkedType;
        if (indexType == null)
          throw new OIndexException(
              "Linked type was not provided."
                  + " You should provide linked type for embedded collections that are going to be indexed.");
      }
      indexDefinition = new OPropertyListIndexDefinition(className, fieldName, indexType);
    } else if (type.equals(OType.LINKBAG)) {
      indexDefinition = new OPropertyRidBagIndexDefinition(className, fieldName);
    } else {
      indexDefinition = new OPropertyIndexDefinition(className, fieldName, type);
    }
    if (collate != null) {
      indexDefinition.setCollate(collate);
    }
    return indexDefinition;
  }

  private static OPropertyMapIndexDefinition.INDEX_BY extractMapIndexSpecifier(
      final String fieldName) {
    final String[] fieldNameParts = FILED_NAME_PATTERN.split(fieldName);
    if (fieldNameParts.length == 1) {
      return OPropertyMapIndexDefinition.INDEX_BY.KEY;
    }
    if (fieldNameParts.length == 3) {

      if ("by".equals(fieldNameParts[1].toLowerCase()))
        try {
          return OPropertyMapIndexDefinition.INDEX_BY.valueOf(fieldNameParts[2].toUpperCase());
        } catch (IllegalArgumentException iae) {
          throw new IllegalArgumentException(
              "Illegal field name format, should be '<property> [by key|value]' but was '"
                  + fieldName
                  + '\'',
              iae);
        }
    }
    throw new IllegalArgumentException(
        "Illegal field name format, should be '<property> [by key|value]' but was '"
            + fieldName
            + '\'');
  }

  private static String adjustFieldName(final OClass clazz, final String fieldName) {
    final OProperty property = clazz.getProperty(fieldName);
    if (property != null) {
      return property.getName();
    } else {
      return fieldName;
    }
  }
}
