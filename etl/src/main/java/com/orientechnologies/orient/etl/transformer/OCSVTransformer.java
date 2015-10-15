/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.etl.OETLProcessor;
import sun.misc.FloatConsts;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @deprecated use OCSVExtractor
 */
@Deprecated
public class OCSVTransformer extends OAbstractTransformer {
  private char         separator          = ',';
  private boolean      columnsOnFirstLine = true;
  private List<String> columnNames        = null;
  private List<OType>  columnTypes        = null;
  private long         skipFrom           = -1;
  private long         skipTo             = -1;
  private long         line               = -1;
  private String       nullValue;
  private Character    stringCharacter    = '"';
  private boolean      unicode            = true;

  public static boolean isFinite(final float value) {
    return Math.abs(value) <= FloatConsts.MAX_VALUE;
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters()
        + ",{separator:{optional:true,description:'Column separator'}},"
        + "{columnsOnFirstLine:{optional:true,description:'Columns are described in the first line'}},"
        + "{columns:{optional:true,description:'Columns array containing names, and optionally type after :'}},"
        + "{nullValue:{optional:true,description:'Value to consider as NULL. Default is not declared'}},"
        + "{unicode:{optional:true,description:'Support unicode values as \\u<code>'}},"
        + "{stringCharacter:{optional:true,description:'String character delimiter. Use \"\" to do not use any delimitator'}},"
        + "{skipFrom:{optional:true,description:'Line number where start to skip',type:'int'}},"
        + "{skipTo:{optional:true,description:'Line number where skip ends',type:'int'}}"
        + "],input:['String'],output:'ODocument'}");
  }

  @Override
  public void configure(final OETLProcessor iProcessor, final ODocument iConfiguration, final OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    if (iConfiguration.containsField("separator"))
      separator = iConfiguration.field("separator").toString().charAt(0);
    if (iConfiguration.containsField("columnsOnFirstLine"))
      columnsOnFirstLine = (Boolean) iConfiguration.field("columnsOnFirstLine");
    if (iConfiguration.containsField("columns")) {
      final List<String> columns = iConfiguration.field("columns");
      columnNames = new ArrayList<String>(columns.size());
      columnTypes = new ArrayList<OType>(columns.size());
      for (String c : columns) {
        final String[] parts = c.split(":");

        columnNames.add(parts[0]);
        if (parts.length > 1)
          columnTypes.add(OType.valueOf(parts[1].toUpperCase()));
        else
          columnTypes.add(OType.ANY);
      }
    }
    if (iConfiguration.containsField("skipFrom"))
      skipFrom = ((Number) iConfiguration.field("skipFrom")).longValue();
    if (iConfiguration.containsField("skipTo"))
      skipTo = ((Number) iConfiguration.field("skipTo")).longValue();
    if (iConfiguration.containsField("nullValue"))
      nullValue = iConfiguration.field("nullValue");
    if (iConfiguration.containsField("unicode"))
      unicode = iConfiguration.field("unicode");
    if (iConfiguration.containsField("stringCharacter")) {
      final String value = iConfiguration.field("stringCharacter").toString();
      if (value.isEmpty())
        stringCharacter = null;
      else
        stringCharacter = value.charAt(0);
    }
  }

  @Override
  public String getName() {
    return "csv";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (skipTransform())
      return null;

    log(OETLProcessor.LOG_LEVELS.DEBUG, "parsing=%s", input);

    final List<String> fields = OStringSerializerHelper.smartSplit(input.toString(), new char[] { separator }, 0, -1, false, false,
        false, false, unicode);

    if (!isColumnNamesCorrect(fields))
      return null;

    final ODocument doc = new ODocument();
    for (int i = 0; i < columnNames.size() && i < fields.size(); ++i) {
      final String fieldName = columnNames.get(i);
      Object fieldValue = null;
      try {
        final String fieldStringValue = getCellContent(fields.get(i));
        final OType fieldType = columnTypes != null ? columnTypes.get(i) : null;

        if (fieldType != null && fieldType != OType.ANY) {
          // DEFINED TYPE
          fieldValue = processKnownType(doc, i, fieldName, fieldStringValue, fieldType);
        } else {
          // DETERMINE THE TYPE
          if (fieldStringValue != null)
            fieldValue = determineTheType(fieldStringValue);
        }
        doc.field(fieldName, fieldValue);

      } catch (Exception e) {
        processor.getStats().incrementErrors();
        log(OETLProcessor.LOG_LEVELS.ERROR, "Error on setting document field %s=%s (cause=%s)", fieldName, fieldValue, e.toString());
      }
    }

    log(OETLProcessor.LOG_LEVELS.DEBUG, "document=%s", doc);
    return doc;
  }

  private Object processKnownType(ODocument doc, int i, String fieldName, String fieldStringValue, OType fieldType) {
    Object fieldValue;
    fieldValue = getCellContent(fieldStringValue);
    try {
      fieldValue = OType.convert(fieldValue, fieldType.getDefaultJavaType());
      doc.field(fieldName, fieldValue);
    } catch (Exception e) {
      processor.getStats().incrementErrors();
      log(OETLProcessor.LOG_LEVELS.ERROR, "Error on converting row %d field '%s' (%d), value '%s' (class:%s) to type: %s",
          processor.getExtractor().getProgress(), fieldName, i, fieldValue, fieldValue.getClass().getName(), fieldType);
    }
    return fieldValue;
  }

  private Object determineTheType(String fieldStringValue) {
    Object fieldValue;
    if ((fieldValue = transformToDate(fieldStringValue)) == null)// try maybe Date type
      if ((fieldValue = transformToNumeric(fieldStringValue)) == null)// try maybe Numeric type
        fieldValue = fieldStringValue; // type String
    return fieldValue;
  }

  private Object transformToDate(String fieldStringValue) {
    // DATE
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setLenient(true);
    Object fieldValue;
    try {
      fieldValue = df.parse(fieldStringValue);
    } catch (ParseException pe) {
      fieldValue = null;
    }
    return fieldValue;
  }

  private Object transformToNumeric(final String fieldStringValue) {
    if (fieldStringValue.isEmpty())
      return fieldStringValue;

    final char c = fieldStringValue.charAt(0);
    if (c != '-' && !Character.isDigit(c))
      // NOT A NUMBER FOR SURE
      return fieldStringValue;

    Object fieldValue;
    try {
      if (fieldStringValue.contains(".") || fieldStringValue.contains(",")) {
        String numberAsString = fieldStringValue.replaceAll(",", ".");
        fieldValue = new Float(numberAsString);
        if (!isFinite((Float) fieldValue)) {
          fieldValue = new Double(numberAsString);
        }
      } else
        try {
          fieldValue = new Integer(fieldStringValue);
        } catch (Exception e) {
          fieldValue = new Long(fieldStringValue);
        }
    } catch (NumberFormatException nf) {
      fieldValue = fieldStringValue;
    }
    return fieldValue;
  }

  private boolean isColumnNamesCorrect(List<String> fields) {
    if (columnNames == null) {
      if (!columnsOnFirstLine)
        throw new OTransformException(getName() + ": columnsOnFirstLine=false and no columns declared");
      columnNames = fields;

      // REMOVE ANY STRING CHARACTERS IF ANY
      for (int i = 0; i < columnNames.size(); ++i)
        columnNames.set(i, getCellContent(columnNames.get(i)));

      return false;
    }

    if (columnsOnFirstLine && line == 0)
      // JUST SKIP FIRST LINE
      return false;

    return true;
  }

  private boolean skipTransform() {
    line++;

    if (skipFrom > -1) {
      if (skipTo > -1) {
        if (line >= skipFrom && line <= skipTo)
          return true;
      } else if (line >= skipFrom)
        // SKIP IT
        return true;
    }
    return false;
  }

  /**
   * Backport copy of Float.isFinite() method that was introduced since Java 1.8 but we must support 1.6. TODO replace after
   * choosing Java 1.8 as minimal supported
   **/
  protected boolean isFinite(Float f) {
    return Math.abs(f) <= FloatConsts.MAX_VALUE;
  }

  // TODO Test, and double doubleqoutes case
  public String getCellContent(String iValue) {
    if (iValue == null || iValue.isEmpty() || "NULL".equals(iValue))
      return null;

    if (stringCharacter != null && iValue.length() > 1
        && (iValue.charAt(0) == stringCharacter && iValue.charAt(iValue.length() - 1) == stringCharacter))
      return iValue.substring(1, iValue.length() - 1);

    return iValue;
  }
}
