package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.orientechnologies.orient.etl.OExtractedItem;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import sun.misc.FloatConsts;

import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.orientechnologies.orient.etl.OETLProcessor.LOG_LEVELS.DEBUG;

/**
 * An extractor based on Apache Commons CSV
 * Created by frank on 10/5/15.
 */
public class OCSVExtractor extends OAbstractSourceExtractor {

  private static String       NULL_STRING     = "NULL";
  protected OExtractedItem    next;
  private Map<String, OType>  columnTypes     = new HashMap<String, OType>();
  private long                skipFrom        = -1;
  private long                skipTo          = -1;
  private Character           stringCharacter = '"';
  private boolean             unicode         = true;
  private Iterator<CSVRecord> recordIterator;
  private CSVFormat           csvFormat;
  private String              nullValue       = NULL_STRING;
  private String              dateFormat      = "yyyy-MM-dd";

  @Override
  public ODocument getConfiguration() {
    return new ODocument()
        .fromJSON("{parameters:["
            + getCommonConfigurationParameters()
            + ",{separator:{optional:true,description:'Column separator'}},"
            + "{columnsOnFirstLine:{optional:true,description:'Columns are described in the first line'}},"
            + "{columns:{optional:true,description:'Columns array containing names, and optionally type after : (e.g.: name:String, age:int'}},"
            + "{nullValue:{optional:true,description:'Value to consider as NULL_STRING. Default is NULL'}},"
            + "{dateFormat:{optional:true,description:'Date format used to parde dates. Default is yyyy-MM-dd'}},"
            + "{quote:{optional:true,description:'String character delimiter. Use \"\" to do not use any delimitator'}},"
            + "{ignoreEmptyLines:{optional:true,description:'Ignore empty lines',type:'boolean'}},"
            + "{skipFrom:{optional:true,description:'Line number where start to skip',type:'int'}},"
            + "{skipTo:{optional:true,description:'Line number where skip ends',type:'int'}}"
            + "{predefinedFormat:{optional:true,description:'Name of standard csv format (from Apache commons-csv): DEFAULT, EXCEL, MYSQL, RFC4180, TDF',type:'String'}}"
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
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    csvFormat = CSVFormat.newFormat(',').withNullString(NULL_STRING).withEscape('\\').withQuote('"');

    if (iConfiguration.containsField("predefinedFormat")) {
      csvFormat = CSVFormat.valueOf(iConfiguration.<String> field("predefinedFormat").toUpperCase());
    }

    if (iConfiguration.containsField("separator")) {
      csvFormat = csvFormat.withDelimiter(iConfiguration.field("separator").toString().charAt(0));
    }

    if (iConfiguration.containsField("dateFormat")) {
      dateFormat = iConfiguration.<String> field("dateFormat");
    }

    if (iConfiguration.containsField("ignoreEmptyLines")) {
      boolean ignoreEmptyLines = iConfiguration.field("ignoreEmptyLines");
      csvFormat = csvFormat.withIgnoreEmptyLines(ignoreEmptyLines);
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
      for (String c : columns) {
        final String[] parts = c.split(":");
        columnNames.add(parts[0]);
        if (parts.length > 1) {
          columnTypes.put(parts[0], OType.valueOf(parts[1].toUpperCase()));
        } else {
          columnTypes.put(parts[0], OType.ANY);
        }
      }

      log(OETLProcessor.LOG_LEVELS.INFO, "column types: %s", columnTypes);
      csvFormat = csvFormat.withHeader(columnNames.toArray(new String[] {}));

    }
    if (iConfiguration.containsField("skipFrom")) {
      skipFrom = ((Number) iConfiguration.field("skipFrom")).longValue();

    }
    if (iConfiguration.containsField("skipTo")) {
      skipTo = ((Number) iConfiguration.field("skipTo")).longValue();
    }

    if (iConfiguration.containsField("nullValue")) {
      nullValue = iConfiguration.<String> field("nullValue");
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
      throw new OExtractorException(e);
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

  private OExtractedItem fetchNext(CSVRecord csvRecord) {
    ODocument doc = new ODocument();
    final Map<String, String> recordAsMap = csvRecord.toMap();

    if (columnTypes.isEmpty()) {

      for (Map.Entry<String, String> en : recordAsMap.entrySet()) {

        final String value = en.getValue();
        if (value == null || nullValue.equals(value) || value.isEmpty())
          doc.field(en.getKey(), null, OType.ANY);
        else
          doc.field(en.getKey(), determineTheType(value));
      }

    } else {

      for (Map.Entry<String, OType> typeEntry : columnTypes.entrySet()) {
        final String fieldName = typeEntry.getKey();
        final OType fieldType = typeEntry.getValue();
        String fieldValueAsString = recordAsMap.get(fieldName);
        try {

          Object fieldValue = OType.convert(fieldValueAsString, fieldType.getDefaultJavaType());
          doc.field(fieldName, fieldValue);
        } catch (Exception e) {
          processor.getStats().incrementErrors();
          log(OETLProcessor.LOG_LEVELS.ERROR, "Error on converting row %d field '%s' (%d), value '%s' (class:%s) to type: %s",
              csvRecord.getRecordNumber(), fieldName, fieldValueAsString, fieldValueAsString.getClass().getName(), fieldType);
        }
      }
    }

    log(DEBUG, "document=%s", doc);
    current++;
    return new OExtractedItem(current, doc);
  }

  @Override
  public OExtractedItem next() {
    return next;
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
      } else {
        try {
          fieldValue = new Integer(fieldStringValue);
        } catch (Exception e) {
          fieldValue = new Long(fieldStringValue);
        }
      }
    } catch (NumberFormatException nf) {
      fieldValue = fieldStringValue;
    }
    return fieldValue;
  }

  /**
   * Backport copy of Float.isFinite() method that was introduced since Java 1.8 but we must support 1.6. TODO replace after
   * choosing Java 1.8 as minimal supported
   **/
  protected boolean isFinite(Float f) {
    return Math.abs(f) <= FloatConsts.MAX_VALUE;
  }

}
