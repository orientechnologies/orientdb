/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLExtractedItem;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** An extractor based on Apache Commons CSV Created by frank on 10/5/15. */
public class OETLCSVExtractor extends OETLAbstractSourceExtractor {

  private static String NULL_STRING = "NULL";
  protected OETLExtractedItem next;
  private Map<String, OType> columnTypes = new HashMap<String, OType>();
  private long skipFrom = -1;
  private long skipTo = -1;
  private Character stringCharacter = '"';
  private boolean unicode = true;
  private Iterator<CSVRecord> recordIterator;
  private CSVFormat csvFormat;
  private String nullValue = NULL_STRING;
  private String dateFormat = "yyyy-MM-dd";
  private String dateTimeFormat = "yyyy-MM-dd hh:mm";

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON(
            "{parameters:["
                + getCommonConfigurationParameters()
                + ",{separator:{optional:true,description:'Column separator'}},"
                + "{columnsOnFirstLine:{optional:true,description:'Columns are described in the first line'}},"
                + "{columns:{optional:true,description:'Columns array containing names, and optionally type after : (e.g.: name:String, age:int'}},"
                + "{nullValue:{optional:true,description:'Value to consider as NULL_STRING. Default is NULL'}},"
                + "{dateFormat:{optional:true,description:'Date format used to parde dates. Default is yyyy-MM-dd'}},"
                + "{dateTimeFormat:{optional:true,description:'DateTime format used to parde dates. Default is yyyy-mm-dd HH:MM'}},"
                + "{quote:{optional:true,description:'String character delimiter. Use \"\" to do not use any delimitator'}},"
                + "{ignoreEmptyLines:{optional:true,description:'Ignore empty lines',type:'boolean'}},"
                + "{ignoreMissingColumns:{optional:true,description:'Ignore empty columns',type:'boolean'}},"
                + "{skipFrom:{optional:true,description:'Line number where start to skip',type:'int'}},"
                + "{skipTo:{optional:true,description:'Line number where skip ends',type:'int'}},"
                + "{predefinedFormat:{optional:true,description:'Name of standard csv format (from Apache commons-csv): Default, Excel, MySQL, RFC4180, TDF',type:'String'}}"
                + "],input:['String'],output:'ODocument'}");
  }

  @Override
  public String getName() {
    return "csv";
  }

  @Override
  public String getUnit() {
    return "rows";
  }

  @Override
  public void configure(ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iConfiguration, iContext);

    csvFormat =
        CSVFormat.newFormat(',')
            .withNullString(NULL_STRING)
            .withEscape('\\')
            .withQuote('"')
            .withCommentMarker('#');

    if (iConfiguration.containsField("predefinedFormat")) {
      csvFormat = CSVFormat.valueOf(iConfiguration.<String>field("predefinedFormat"));
    }

    if (iConfiguration.containsField("separator")) {
      csvFormat = csvFormat.withDelimiter(iConfiguration.field("separator").toString().charAt(0));
    }

    if (iConfiguration.containsField("dateFormat")) {
      dateFormat = iConfiguration.<String>field("dateFormat");
    }
    if (iConfiguration.containsField("dateTimeFormat")) {
      dateTimeFormat = iConfiguration.<String>field("dateTimeFormat");
    }

    if (iConfiguration.containsField("ignoreEmptyLines")) {
      boolean ignoreEmptyLines = iConfiguration.field("ignoreEmptyLines");
      csvFormat = csvFormat.withIgnoreEmptyLines(ignoreEmptyLines);
    }

    if (iConfiguration.containsField("ignoreMissingColumns")) {
      boolean ignoreMissingColumns = iConfiguration.field("ignoreMissingColumns");
      csvFormat = csvFormat.withAllowMissingColumnNames(ignoreMissingColumns);
    }

    if (iConfiguration.containsField("columnsOnFirstLine")) {
      Boolean columnsOnFirstLine = (Boolean) iConfiguration.field("columnsOnFirstLine");
      if (columnsOnFirstLine.equals(Boolean.TRUE)) {
        csvFormat = csvFormat.withHeader();
      }
    } else {
      csvFormat = csvFormat.withHeader();
    }

    if (iConfiguration.containsField("columns")) {
      final List<String> columns = iConfiguration.field("columns");

      ArrayList<String> columnNames = new ArrayList<String>(columns.size());
      columnTypes = new HashMap<String, OType>(columns.size());
      for (String col : columns) {
        final String[] col2Type = col.split(":");
        columnNames.add(col2Type[0]);
        if (col2Type.length > 1) {
          columnTypes.put(col2Type[0], OType.valueOf(col2Type[1].toUpperCase(Locale.ENGLISH)));
        } else {
          columnTypes.put(col2Type[0], OType.ANY);
        }
      }

      log(Level.INFO, "column types: %s", columnTypes);
      csvFormat = csvFormat.withHeader(columnNames.toArray(new String[] {})).withSkipHeaderRecord();
    }

    if (iConfiguration.containsField("skipFrom")) {
      skipFrom = ((Number) iConfiguration.field("skipFrom")).longValue();
    }

    if (iConfiguration.containsField("skipTo")) {
      skipTo = ((Number) iConfiguration.field("skipTo")).longValue();
    }

    if (iConfiguration.containsField("nullValue")) {
      nullValue = iConfiguration.<String>field("nullValue");
      csvFormat = csvFormat.withNullString(nullValue);
    }

    if (iConfiguration.containsField("quote")) {
      final String value = iConfiguration.field("quote").toString();
      if (!value.isEmpty()) {
        csvFormat = csvFormat.withQuote(value.charAt(0));
      }
    }
  }

  @Override
  public void extract(final Reader iReader) {
    super.extract(iReader);
    try {

      CSVParser parser = new CSVParser(iReader, csvFormat);

      recordIterator = parser.iterator();
    } catch (IOException e) {
      throw new OETLExtractorException(e);
    }
  }

  @Override
  public boolean hasNext() {
    if (recordIterator.hasNext()) {
      CSVRecord csvRecord = recordIterator.next();

      while (shouldSkipRecord(csvRecord) && recordIterator.hasNext()) {
        csvRecord = recordIterator.next();
      }
      next = fetchNext(csvRecord);
      return true;
    }
    return false;
  }

  private boolean shouldSkipRecord(CSVRecord csvRecord) {
    return csvRecord.getRecordNumber() <= skipTo && csvRecord.getRecordNumber() >= skipFrom;
  }

  private OETLExtractedItem fetchNext(CSVRecord csvRecord) {
    ODocument doc = new ODocument();
    final Map<String, String> recordAsMap = csvRecord.toMap();

    if (columnTypes.isEmpty()) {

      for (Map.Entry<String, String> en : recordAsMap.entrySet()) {

        final String value = en.getValue();
        if (!csvFormat.getAllowMissingColumnNames() || !en.getKey().isEmpty()) {
          if (value == null || nullValue.equals(value) || value.isEmpty())
            doc.field(en.getKey(), null, OType.ANY);
          else doc.field(en.getKey(), determineTheType(value));
        }
      }

    } else {

      for (Map.Entry<String, OType> typeEntry : columnTypes.entrySet()) {
        final String fieldName = typeEntry.getKey();
        final OType fieldType = typeEntry.getValue();
        String fieldValueAsString = recordAsMap.get(fieldName);
        try {
          if (fieldType != null
              && fieldType.getDefaultJavaType() != null
              && fieldType.getDefaultJavaType().equals(Date.class)) {
            if (fieldType.equals(OType.DATE))
              doc.field(fieldName, transformToDate(fieldValueAsString));
            else doc.field(fieldName, transformToDateTime(fieldValueAsString));
          } else {
            Object fieldValue = OType.convert(fieldValueAsString, fieldType.getDefaultJavaType());
            doc.field(fieldName, fieldValue);
          }
        } catch (Exception e) {
          processor.getStats().incrementErrors();
          log(
              Level.SEVERE,
              "Error on converting row %d field '%s' (%d), value '%s' (class:%s) to type: %s",
              csvRecord.getRecordNumber(),
              fieldName,
              fieldValueAsString,
              fieldValueAsString.getClass().getName(),
              fieldType);
        }
      }
    }

    log(Level.FINE, "document=%s", doc);
    current++;
    return new OETLExtractedItem(current, doc);
  }

  private Object determineTheType(String fieldStringValue) {
    Object fieldValue;
    if ((fieldValue = transformToDate(fieldStringValue)) == null) // try maybe Date type
    if ((fieldValue = transformToNumeric(fieldStringValue)) == null) // try maybe Numeric type
      if ((fieldValue = transformToBoolean(fieldStringValue)) == null) // try maybe Boolean type
        fieldValue = fieldStringValue; // type String
    return fieldValue;
  }

  private Object transformToDate(String fieldStringValue) {
    // DATE
    DateFormat df = new SimpleDateFormat(dateFormat);
    df.setLenient(true);
    Object fieldValue;
    try {
      fieldValue = df.parse(fieldStringValue);
    } catch (ParseException pe) {
      fieldValue = null;
    }
    return fieldValue;
  }

  private Object transformToDateTime(String fieldStringValue) {
    // DATE
    DateFormat df = new SimpleDateFormat(dateTimeFormat);
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
    if (fieldStringValue.isEmpty()) return null;

    final char c = fieldStringValue.charAt(0);
    if (c != '-' && !Character.isDigit(c))
      // NOT A NUMBER FOR SURE
      return null;

    Object fieldValue;
    try {
      if (fieldStringValue.contains(".") || fieldStringValue.contains(",")) {
        String numberAsString = fieldStringValue.replaceAll(",", ".");
        fieldValue = new Float(numberAsString);
        if (!isFinite((Float) fieldValue)) {
          fieldValue = new Double(numberAsString);
        }
      } else {
        try {
          fieldValue = new Integer(fieldStringValue);
        } catch (Exception e) {
          fieldValue = new Long(fieldStringValue);
        }
      }
    } catch (NumberFormatException nf) {
      fieldValue = null;
    }
    return fieldValue;
  }

  private Object transformToBoolean(final String fieldStringValue) {
    if (fieldStringValue.equalsIgnoreCase(Boolean.FALSE.toString())
        || fieldStringValue.equalsIgnoreCase(Boolean.TRUE.toString()))
      return Boolean.parseBoolean(fieldStringValue);
    return null;
  }

  /**
   * Backport copy of Float.isFinite() method that was introduced since Java 1.8 but we must support
   * 1.6. TODO replace after choosing Java 1.8 as minimal supported
   */
  protected boolean isFinite(Float f) {
    return Math.abs(f) <= Float.MAX_VALUE;
  }

  @Override
  public OETLExtractedItem next() {
    return next;
  }
}
