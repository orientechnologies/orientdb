/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.parser.OBaseParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerCSVAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemAbstract;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemField;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemParameter;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItemVariable;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionRuntime;
import com.orientechnologies.orient.core.sql.method.OSQLMethod;
import com.orientechnologies.orient.core.sql.method.OSQLMethodFactory;

/**
 * SQL Helper class
 * 
 * @author Luca Garulli
 * 
 */
public class OSQLHelper {
  public static final String NAME              = "sql";

  public static final String VALUE_NOT_PARSED  = "_NOT_PARSED_";
  public static final String NOT_NULL          = "_NOT_NULL_";
  public static final String DEFINED           = "_DEFINED_";

  private static ClassLoader orientClassLoader = OSQLFilterItemAbstract.class.getClassLoader();

  /**
   * Convert fields from text to real value. Supports: String, RID, Boolean, Float, Integer and NULL.
   * 
   * @param iDatabase
   * @param iValue
   *          Value to convert.
   * @return The value converted if recognized, otherwise VALUE_NOT_PARSED
   */
  public static Object parseValue(String iValue, final OCommandContext iContext) {
    if (iValue == null)
      return null;

    iValue = iValue.trim();

    Object fieldValue = VALUE_NOT_PARSED;

    if (iValue.startsWith("'") && iValue.endsWith("'") || iValue.startsWith("\"") && iValue.endsWith("\""))
      // STRING
      fieldValue = OStringSerializerHelper.getStringContent(iValue);
    else if (iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
        && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.LIST_END) {
      // COLLECTION/ARRAY
      final List<String> items = OStringSerializerHelper.smartSplit(iValue.substring(1, iValue.length() - 1),
          OStringSerializerHelper.RECORD_SEPARATOR);

      final List<Object> coll = new ArrayList<Object>();
      for (String item : items) {
        coll.add(parseValue(item, iContext));
      }
      fieldValue = coll;

    } else if (iValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
        && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.MAP_END) {
      // MAP
      final List<String> items = OStringSerializerHelper.smartSplit(iValue.substring(1, iValue.length() - 1),
          OStringSerializerHelper.RECORD_SEPARATOR);

      final Map<Object, Object> map = new HashMap<Object, Object>();
      for (String item : items) {
        final List<String> parts = OStringSerializerHelper.smartSplit(item, OStringSerializerHelper.ENTRY_SEPARATOR);

        if (parts == null || parts.size() != 2)
          throw new OCommandSQLParsingException("Map found but entries are not defined as <key>:<value>");

        map.put(parseValue(parts.get(0), iContext), parseValue(parts.get(1), iContext));
      }

      if (map.containsKey(ODocumentHelper.ATTRIBUTE_TYPE))
        // IT'S A DOCUMENT
        fieldValue = new ODocument(map);
      else
        fieldValue = map;
    } else if (iValue.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN
        && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.EMBEDDED_END) {
      // SUB-COMMAND
      fieldValue = new OCommandSQL(iValue.substring(1, iValue.length() - 1));
      ((OCommandSQL) fieldValue).getContext().setParent(iContext);

    } else if (iValue.charAt(0) == ORID.PREFIX)
      // RID
      fieldValue = new ORecordId(iValue.trim());
    else {

      final String upperCase = iValue.toUpperCase(Locale.ENGLISH);
      if (upperCase.equals("NULL"))
        // NULL
        fieldValue = null;
      else if (upperCase.equals("NOT NULL"))
        // NULL
        fieldValue = NOT_NULL;
      else if (upperCase.equals("DEFINED"))
        // NULL
        fieldValue = DEFINED;
      else if (upperCase.equals("TRUE"))
        // BOOLEAN, TRUE
        fieldValue = Boolean.TRUE;
      else if (upperCase.equals("FALSE"))
        // BOOLEAN, FALSE
        fieldValue = Boolean.FALSE;
      else {
        final Object v = parseStringNumber(iValue);
        if (v != null)
          fieldValue = v;
      }
    }

    return fieldValue;
  }

  public static Object parseStringNumber(final String iValue) {
    final OType t = ORecordSerializerCSVAbstract.getType(iValue);

    if (t == OType.INTEGER)
      return Integer.parseInt(iValue);
    else if (t == OType.LONG)
      return Long.parseLong(iValue);
    else if (t == OType.FLOAT)
      return Float.parseFloat(iValue);
    else if (t == OType.SHORT)
      return Short.parseShort(iValue);
    else if (t == OType.BYTE)
      return Byte.parseByte(iValue);
    else if (t == OType.DOUBLE)
      return Double.parseDouble(iValue);
    else if (t == OType.DATE || t == OType.DATETIME)
      return new Date(Long.parseLong(iValue));

    return null;
  }

  public static Object parseValue(final OSQLPredicate iSQLFilter, final OBaseParser iCommand, final String iWord,
      final OCommandContext iContext) {
    if (iWord.charAt(0) == OStringSerializerHelper.PARAMETER_POSITIONAL
        || iWord.charAt(0) == OStringSerializerHelper.PARAMETER_NAMED) {
      if (iSQLFilter != null)
        return iSQLFilter.addParameter(iWord);
      else
        return new OSQLFilterItemParameter(iWord);
    } else
      return parseValue(iCommand, iWord, iContext);
  }

  public static Object parseValue(final OBaseParser iCommand, final String iWord, final OCommandContext iContext) {
    if (iWord.equals("*"))
      return "*";

    // TRY TO PARSE AS RAW VALUE
    final Object v = parseValue(iWord, iContext);
    if (v != VALUE_NOT_PARSED)
      return v;

    if (!iWord.equalsIgnoreCase("any()") && !iWord.equalsIgnoreCase("all()")) {
      // TRY TO PARSE AS FUNCTION
      final Object func = OSQLHelper.getFunction(iCommand, iWord);
      if (func != null)
        return func;
    }

    if (iWord.startsWith("$"))
      // CONTEXT VARIABLE
      return new OSQLFilterItemVariable(iCommand, iWord);

    // PARSE AS FIELD
    return new OSQLFilterItemField(iCommand, iWord);
  }

  public static OSQLFunctionRuntime getFunction(final OBaseParser iCommand, final String iWord) {
    final int separator = iWord.indexOf('.');
    final int beginParenthesis = iWord.indexOf(OStringSerializerHelper.EMBEDDED_BEGIN);
    if (beginParenthesis > -1 && (separator == -1 || separator > beginParenthesis)) {
      final int endParenthesis = iWord.indexOf(OStringSerializerHelper.EMBEDDED_END, beginParenthesis);

      if (endParenthesis > -1 && Character.isLetter(iWord.charAt(0)))
        // FUNCTION: CREATE A RUN-TIME CONTAINER FOR IT TO SAVE THE PARAMETERS
        return new OSQLFunctionRuntime(iCommand, iWord);
    }

    return null;
  }

  public static Object getValue(final Object iObject) {
    if (iObject == null)
      return null;

    if (iObject instanceof OSQLFilterItem)
      return ((OSQLFilterItem) iObject).getValue(null, null);

    return iObject;
  }

  public static Object getValue(final Object iObject, final ORecordInternal<?> iRecord) {
    if (iObject == null)
      return null;

    if (iObject instanceof OSQLFilterItem)
      return ((OSQLFilterItem) iObject).getValue(iRecord, null);

    return iObject;
  }

  public static Object getValue(final Object iObject, final ORecordInternal<?> iRecord, final OCommandContext iContext) {
    if (iObject == null)
      return null;

    if (iObject instanceof OSQLFilterItem)
      return ((OSQLFilterItem) iObject).getValue(iRecord, iContext);
    else if (iObject instanceof String) {
      final String s = ((String) iObject).trim();
      if (!s.isEmpty() && !OIOUtils.isStringContent(iObject) && !Character.isDigit(s.charAt(0)))
        // INTERPRETS IT
        return ODocumentHelper.getFieldValue(iRecord, s, iContext);
    }

    return iObject;
  }

  public static Object resolveFieldValue(final ODocument iDocument, final String iFieldName, final Object iFieldValue,
      final OCommandParameters iArguments) {
    if (iFieldValue instanceof OSQLFilterItemField) {
      final OSQLFilterItemField f = (OSQLFilterItemField) iFieldValue;
      if (f.getRoot().equals("?"))
        // POSITIONAL PARAMETER
        return iArguments.getNext();
      else if (f.getRoot().startsWith(":"))
        // NAMED PARAMETER
        return iArguments.getByName(f.getRoot().substring(1));
    }

    if (iFieldValue instanceof ODocument && !((ODocument) iFieldValue).getIdentity().isValid())
      // EMBEDDED DOCUMENT
      ((ODocument) iFieldValue).addOwner(iDocument);

    return OSQLHelper.getValue(iFieldValue, iDocument);
  }

  public static Set<ODocument> bindParameters(final ODocument iDocument, final Map<String, Object> iFields,
      final OCommandParameters iArguments, final OCommandContext iContext) {
    if (iFields == null)
      return null;

    Set<ODocument> changedDocuments = null;

    // BIND VALUES
    for (Entry<String, Object> field : iFields.entrySet()) {
      final String fieldName = field.getKey();
      Object fieldValue = field.getValue();

      if (fieldValue != null) {
        if (fieldValue instanceof OCommandSQL) {
          final OCommandSQL cmd = (OCommandSQL) fieldValue;
          cmd.getContext().setParent(iContext);
          fieldValue = ODatabaseRecordThreadLocal.INSTANCE.get().command(cmd).execute();

          // CHECK FOR CONVERSIONS
          if (iDocument.getSchemaClass() != null) {
            final OProperty prop = iDocument.getSchemaClass().getProperty(fieldName);
            if (prop != null) {
              if (prop.getType() == OType.LINK) {
                if (OMultiValue.isMultiValue(fieldValue) && OMultiValue.getSize(fieldValue) == 1)
                  // GET THE FIRST ITEM AS UNIQUE LINK
                  fieldValue = OMultiValue.getFirstValue(fieldValue);
              }
            }
          }

          if (OMultiValue.isMultiValue(fieldValue)) {
            final List<Object> tempColl = new ArrayList<Object>(OMultiValue.getSize(fieldValue));

            String singleFieldName = null;
            for (Object o : OMultiValue.getMultiValueIterable(fieldValue)) {
              if (o instanceof OIdentifiable && !((OIdentifiable) o).getIdentity().isPersistent()) {
                // TEMPORARY / EMBEDDED
                final ORecord<?> rec = ((OIdentifiable) o).getRecord();
                if (rec != null && rec instanceof ODocument) {
                  // CHECK FOR ONE FIELD ONLY
                  final ODocument doc = (ODocument) rec;
                  if (doc.fields() == 1) {
                    singleFieldName = doc.fieldNames()[0];
                    tempColl.add(doc.field(singleFieldName));
                  } else {
                    // TRANSFORM IT IN EMBEDDED
                    doc.getIdentity().reset();
                    doc.addOwner(iDocument);
                    tempColl.add(doc);
                  }
                }
              }
            }

            fieldValue = tempColl;
          }
        }
      }

      final ODocument doc = iDocument.field(fieldName, resolveFieldValue(iDocument, fieldName, fieldValue, iArguments));
      if (doc != null) {
        if (changedDocuments == null)
          changedDocuments = new HashSet<ODocument>();
        changedDocuments.add(doc);
      }
    }
    return changedDocuments;
  }

  public static String[] getAllMethodNames() {
    final List<String> methods = new ArrayList<String>();
    final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, orientClassLoader);
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      methods.addAll(factory.getMethodNames());
    }
    return methods.toArray(new String[methods.size()]);
  }

  public static OSQLMethod getMethodByName(String name) {
    name = name.toLowerCase();
    final Iterator<OSQLMethodFactory> ite = lookupProviderWithOrientClassLoader(OSQLMethodFactory.class, orientClassLoader);
    while (ite.hasNext()) {
      final OSQLMethodFactory factory = ite.next();
      if (factory.hasMethod(name)) {
        return factory.createMethod(name);
      }
    }
    return null;
  }
}
