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
package com.orientechnologies.orient.console;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.console.OConsoleApplication;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class OTableFormatter {
  protected final static String      MORE            = "...";
  protected final static Set<String> prefixedColumns = new LinkedHashSet<String>(
      Arrays.asList(new String[] { "#", "@RID", "@CLASS" }));
  protected final OConsoleApplication out;
  protected final SimpleDateFormat DEF_DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  protected       int              minColumnSize  = 4;
  protected       int              maxWidthSize   = 150;

  public OTableFormatter(final OConsoleApplication iConsole) {
    this.out = iConsole;
  }

  public void writeRecords(final Collection<OIdentifiable> resultSet, final int limit) {
    writeRecords(resultSet, limit, null);
  }

  public void writeRecords(final Collection<OIdentifiable> resultSet, final int limit,
      final OCallable<Object, OIdentifiable> iAfterDump) {
    final Map<String, Integer> columns = parseColumns(resultSet, limit);

    int fetched = 0;
    for (OIdentifiable record : resultSet) {
      dumpRecordInTable(fetched++, record, columns);
      if (iAfterDump != null)
        iAfterDump.call(record);

      if (limit > -1 && fetched >= limit) {
        printHeaderLine(columns);
        out.message("\nLIMIT EXCEEDED: resultset contains more items not displayed (limit=" + limit + ")");
        return;
      }
    }

    if (fetched > 0)
      printHeaderLine(columns);
  }

  public int getMaxWidthSize() {
    return maxWidthSize;
  }

  public OTableFormatter setMaxWidthSize(int maxWidthSize) {
    this.maxWidthSize = maxWidthSize;
    return this;
  }

  public void dumpRecordInTable(final int iIndex, final OIdentifiable iRecord, final Map<String, Integer> iColumns) {
    if (iIndex == 0)
      printHeader(iColumns);

    // FORMAT THE LINE DYNAMICALLY
    List<Object> vargs = new ArrayList<Object>();
    try {
      if (iRecord instanceof ODocument)
        ((ODocument) iRecord).setLazyLoad(false);

      final StringBuilder format = new StringBuilder(maxWidthSize);
      for (Entry<String, Integer> col : iColumns.entrySet()) {
        if (format.length() > 0)
          format.append('|');
        format.append("%-" + col.getValue() + "s");

        Object value = getFieldValue(iIndex, iRecord, col.getKey());

        if (value != null) {
          if (value instanceof ORidBag) {
            ((ORidBag) value).size();
          }

          value = value.toString();
          if (((String) value).length() > col.getValue()) {
            // APPEND ...
            value = ((String) value).substring(0, col.getValue() - 3) + MORE;
          }
        }

        vargs.add(value);
      }

      out.message("\n" + format.toString(), vargs.toArray());

    } catch (Throwable t) {
      out.message("%3d|%9s|%s\n", iIndex, iRecord.getIdentity(), "Error on loading record dued to: " + t);
    }
  }

  private Object getFieldValue(final int iIndex, final OIdentifiable iRecord, final String iColumnName) {
    Object value = null;

    if (iColumnName.equals("#"))
      // RECORD NUMBER
      value = iIndex;
    else if (iColumnName.equals("@RID"))
      // RID
      value = iRecord.getIdentity().toString();
    else if (iRecord instanceof ODocument)
      value = ((ODocument) iRecord).field(iColumnName);
    else if (iRecord instanceof ORecordBytes)
      value = "<binary> (size=" + ((ORecordBytes) iRecord).toStream().length + " bytes)";
    else if (iRecord instanceof OIdentifiable) {
      final ORecord rec = iRecord.getRecord();
      if (rec instanceof ODocument)
        value = ((ODocument) rec).field(iColumnName);
      else if (rec instanceof ORecordBytes)
        value = "<binary> (size=" + ((ORecordBytes) rec).toStream().length + " bytes)";
    }

    if (value instanceof OMultiCollectionIterator<?>)
      value = "[" + ((OMultiCollectionIterator<?>) value).size() + "]";
    else if (value instanceof Collection<?>)
      value = "[" + ((Collection<?>) value).size() + "]";
    else if (value instanceof ORecord) {
      if (((ORecord) value).getIdentity().equals(ORecordId.EMPTY_RECORD_ID)) {
        value = ((ORecord) value).toString();
      } else {
        value = ((ORecord) value).getIdentity().toString();
      }
    } else if (value instanceof Date) {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
      if (db != null)
        value = db.getStorage().getConfiguration().getDateTimeFormatInstance().format((Date) value);
      else {
        value = DEF_DATEFORMAT.format((Date) value);
      }
    } else if (value instanceof byte[])
      value = "byte[" + ((byte[]) value).length + "]";

    return value;
  }

  private void printHeader(final Map<String, Integer> iColumns) {
    final StringBuilder buffer = new StringBuilder("\n");

    printHeaderLine(iColumns);
    int i = 0;
    for (Entry<String, Integer> column : iColumns.entrySet()) {
      if (i++ > 0)
        buffer.append('|');
      String colName = column.getKey();
      if (colName.length() > column.getValue())
        colName = colName.substring(0, column.getValue());
      buffer.append(String.format("%-" + column.getValue() + "s", colName));
    }
    out.message(buffer.toString());

    printHeaderLine(iColumns);
  }

  private void printHeaderLine(final Map<String, Integer> iColumns) {
    final StringBuilder buffer = new StringBuilder("\n");

    if (iColumns.size() > 0) {
      int i = 0;
      for (Entry<String, Integer> col : iColumns.entrySet()) {
        if (i++ > 0)
          buffer.append("+");

        for (int k = 0; k < col.getValue(); ++k)
          buffer.append("-");
      }
    }

    out.message(buffer.toString());
  }

  /**
   * Fill the column map computing the maximum size for a field.
   *
   * @param resultSet
   * @param limit
   * @return
   */
  private Map<String, Integer> parseColumns(final Collection<OIdentifiable> resultSet, final int limit) {
    final Map<String, Integer> columns = new LinkedHashMap<String, Integer>();

    for (String c : prefixedColumns)
      columns.put(c, minColumnSize);

    boolean tempRids = false;

    int fetched = 0;
    for (OIdentifiable id : resultSet) {
      ORecord rec = id.getRecord();

      for (String c : prefixedColumns)
        columns.put(c, getColumnSize(fetched, rec, c, columns.get(c)));

      if (rec instanceof ODocument) {
        ((ODocument) rec).setLazyLoad(false);
        // PARSE ALL THE DOCUMENT'S FIELDS
        ODocument doc = (ODocument) rec;
        for (String fieldName : doc.fieldNames()) {
          columns.put(fieldName, getColumnSize(fetched, doc, fieldName, columns.get(fieldName)));
        }
      } else if (rec instanceof ORecordBytes) {
        // UNIQUE BINARY FIELD
        columns.put("value", maxWidthSize - 15);
      }

      if (!tempRids && !rec.getIdentity().isPersistent())
        tempRids = true;

      if (limit > -1 && fetched++ >= limit)
        break;
    }

    if (tempRids)
      columns.remove("@RID");

    // COMPUTE MAXIMUM WIDTH
    int width = 0;
    for (Entry<String, Integer> col : columns.entrySet())
      width += col.getValue();

    if (width > maxWidthSize) {
      // SCALE COLUMNS AUTOMATICALLY
      final List<Map.Entry<String, Integer>> orderedColumns = new ArrayList<Map.Entry<String, Integer>>();
      orderedColumns.addAll(columns.entrySet());
      Collections.sort(orderedColumns, new Comparator<Map.Entry<String, Integer>>() {

        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
          return o1.getValue().compareTo(o2.getValue());
        }
      });

      // START CUTTING THE BIGGEST ONES
      Collections.reverse(orderedColumns);
      while (width > maxWidthSize) {
        int oldWidth = width;

        for (Map.Entry<String, Integer> entry : orderedColumns) {
          final int redux = entry.getValue() * 10 / 100;

          if (entry.getValue() - redux < minColumnSize)
            // RESTART FROM THE LARGEST COLUMN
            break;

          entry.setValue(entry.getValue() - redux);

          width -= redux;
          if (width <= maxWidthSize)
            break;
        }

        if (width == oldWidth)
          // REACHED THE MINIMUM
          break;
      }

      // POPULATE THE COLUMNS WITH THE REDUXED VALUES
      columns.clear();
      for (String c : prefixedColumns)
        columns.put(c, minColumnSize);
      Collections.reverse(orderedColumns);
      for (Entry<String, Integer> col : orderedColumns)
        // if (!col.getKey().equals("#") && !col.getKey().equals("@RID"))
        columns.put(col.getKey(), col.getValue());

    }

    if (tempRids)
      columns.remove("@RID");

    return columns;
  }

  private Integer getColumnSize(final Integer iIndex, final ORecord iRecord, final String fieldName, final Integer origSize) {
    Integer newColumnSize;
    if (origSize == null)
      // START FROM THE FIELD NAME SIZE
      newColumnSize = fieldName.length();
    else
      newColumnSize = Math.max(origSize, fieldName.length());

    final Object fieldValue = getFieldValue(iIndex, iRecord, fieldName);

    if (fieldValue != null) {
      final String fieldValueAsString = fieldValue.toString();
      if (fieldValueAsString.length() > newColumnSize)
        newColumnSize = fieldValueAsString.length();
    }

    if (newColumnSize < minColumnSize)
      // SET THE MINIMUM SIZE
      newColumnSize = minColumnSize;

    return newColumnSize;
  }
}
