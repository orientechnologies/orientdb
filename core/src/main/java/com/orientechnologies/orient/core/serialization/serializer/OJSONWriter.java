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
package com.orientechnologies.orient.core.serialization.serializer;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.util.ODateHelper;

import java.io.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public class OJSONWriter {
  private static final String DEF_FORMAT = "rid,type,version,class,attribSameRow,indent:2,dateAsLong";
  private final String format;
  private       Writer out;
  private boolean prettyPrint    = false;
  private boolean firstAttribute = true;

  public OJSONWriter(final Writer out) {
    this(out, DEF_FORMAT);
  }

  public OJSONWriter(final Writer out, final String iJsonFormat) {
    this.out = out;
    this.format = iJsonFormat;
    if (iJsonFormat.contains("prettyPrint"))
      prettyPrint = true;
  }

  public static String writeValue(final Object iValue) throws IOException {
    return writeValue(iValue, DEF_FORMAT);
  }

  public static String writeValue(Object iValue, final String iFormat) throws IOException {
    return writeValue(iValue, iFormat, 0);
  }

  public static String writeValue(Object iValue, final String iFormat, final int iIndentLevel) throws IOException {
    if (iValue == null)
      return "null";

    final StringBuilder buffer = new StringBuilder(64);

    final boolean oldAutoConvertSettings;

    if (iValue instanceof ORecordLazyMultiValue) {
      oldAutoConvertSettings = ((ORecordLazyMultiValue) iValue).isAutoConvertToRecord();
      ((ORecordLazyMultiValue) iValue).setAutoConvertToRecord(false);
    } else
      oldAutoConvertSettings = false;

    if (iValue instanceof Boolean || iValue instanceof Number)
      buffer.append(iValue.toString());

    else if (iValue instanceof OIdentifiable) {
      final OIdentifiable linked = (OIdentifiable) iValue;
      if (linked.getIdentity().isValid()) {
        buffer.append('\"');
        linked.getIdentity().toString(buffer);
        buffer.append('\"');
      } else {
        if (iFormat != null && iFormat.contains("shallow"))
          buffer.append("{}");
        else {
          final ORecord rec = linked.getRecord();
          if (rec != null) {
            final String embeddedFormat = iFormat != null && iFormat.isEmpty() ? "indent:" + iIndentLevel : iFormat + ",indent:" + iIndentLevel;
            buffer.append(rec.toJSON(embeddedFormat));
          } else
            buffer.append("null");
        }
      }

    } else if (iValue.getClass().isArray()) {

      if (iValue instanceof byte[]) {
        buffer.append('\"');
        final byte[] source = (byte[]) iValue;

        if (iFormat != null && iFormat.contains("shallow"))
          buffer.append(source.length);
        else
          buffer.append(OBase64Utils.encodeBytes(source));

        buffer.append('\"');
      } else {
        buffer.append('[');
        int size = Array.getLength(iValue);
        if (iFormat != null && iFormat.contains("shallow"))
          buffer.append(size);
        else
          for (int i = 0; i<size; ++i) {
            if (i>0)
              buffer.append(",");
            buffer.append(writeValue(Array.get(iValue, i), iFormat));
          }
        buffer.append(']');

      }
    } else if (iValue instanceof Iterator<?>)
      iteratorToJSON((Iterator<?>) iValue, iFormat, buffer);
    else if (iValue instanceof Iterable<?>)
      iteratorToJSON(((Iterable<?>) iValue).iterator(), iFormat, buffer);

    else if (iValue instanceof Map<?, ?>)
      mapToJSON((Map<Object, Object>) iValue, iFormat, buffer);

    else if (iValue instanceof Map.Entry<?, ?>) {
      final Map.Entry<?, ?> entry = (Entry<?, ?>) iValue;
      buffer.append('{');
      buffer.append(writeValue(entry.getKey(), iFormat));
      buffer.append(":");
      if (iFormat.contains("prettyPrint"))
        buffer.append(' ');
      buffer.append(writeValue(entry.getValue(), iFormat));
      buffer.append('}');
    } else if (iValue instanceof Date) {
      if (iFormat.indexOf("dateAsLong")>-1)
        buffer.append(((Date) iValue).getTime());
      else {
        buffer.append('"');
        buffer.append(ODateHelper.getDateTimeFormatInstance().format(iValue));
        buffer.append('"');
      }
    } else if (iValue instanceof BigDecimal)
      buffer.append(((BigDecimal) iValue).toPlainString());

    else if (iValue instanceof ORecordLazyMultiValue)
      iteratorToJSON(((ORecordLazyMultiValue) iValue).rawIterator(), iFormat, buffer);
    else if (iValue instanceof Iterable<?>)
      iteratorToJSON(((Iterable<?>) iValue).iterator(), iFormat, buffer);

    else {
      OType  t = OType.getTypeByValue(iValue);
      if(t == OType.CUSTOM){
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream object = new ObjectOutputStream(baos);
        object.writeObject(iValue);
        object.flush();
        buffer.append('"');
        buffer.append(OBase64Utils.encodeBytes(baos.toByteArray()));
        buffer.append('"');
      }else {
        // TREAT IT AS STRING
        final String v = iValue.toString();
        buffer.append('"');
        buffer.append(encode(v));
        buffer.append('"');
      }
    }

    if (iValue instanceof ORecordLazyMultiValue)
      ((ORecordLazyMultiValue) iValue).setAutoConvertToRecord(oldAutoConvertSettings);

    return buffer.toString();
  }

  protected static void iteratorToJSON(final Iterator<?> it, final String iFormat, final StringBuilder buffer) throws IOException {
    buffer.append('[');
    if (iFormat != null && iFormat.contains("shallow")) {
      if (it instanceof OMultiCollectionIterator<?>)
        buffer.append(((OMultiCollectionIterator<?>) it).size());
      else {
        // COUNT THE MULTI VALUE
        int i;
        for (i = 0; it.hasNext(); ++i)
          it.next();
        buffer.append(i);
      }
    } else {
      for (int i = 0; it.hasNext(); ++i) {
        if (i>0)
          buffer.append(",");
        buffer.append(writeValue(it.next(), iFormat));
      }
    }
    buffer.append(']');
  }

  public static Object encode(final Object iValue) {
    return OIOUtils.encode(iValue);
  }

  public static String listToJSON(final Collection<? extends OIdentifiable> iRecords, final String iFormat) {
    try {
      final StringWriter buffer = new StringWriter();
      final OJSONWriter json = new OJSONWriter(buffer);
      // WRITE RECORDS
      json.beginCollection(0, false, null);
      if (iRecords != null) {
        if (iFormat != null && iFormat.contains("shallow")) {
          buffer.append("" + iRecords.size());
        } else {
          int counter = 0;
          String objectJson;
          for (OIdentifiable rec : iRecords) {
            if (rec != null)
              try {
                objectJson = iFormat != null ? rec.getRecord().toJSON(iFormat) : rec.getRecord().toJSON();

                if (counter++>0)
                  buffer.append(",");

                buffer.append(objectJson);
              } catch (Exception e) {
                OLogManager.instance().error(json, "Error transforming record " + rec.getIdentity() + " to JSON", e);
              }
          }
        }
      }
      json.endCollection(0, false);

      return buffer.toString();
    } catch (IOException e) {
      throw new OSerializationException("Error on serializing collection", e);
    }
  }

  public static String mapToJSON(Map<?, ?> iMap) {
    return mapToJSON(iMap, null, new StringBuilder(128));
  }

  public static String mapToJSON(final Map<?, ?> iMap, final String iFormat, final StringBuilder buffer) {
    try {
      buffer.append('{');
      if (iMap != null) {
        int i = 0;
        Entry<?, ?> entry;
        for (Iterator<?> it = iMap.entrySet().iterator(); it.hasNext(); ++i) {
          entry = (Entry<?, ?>) it.next();
          if (i>0)
            buffer.append(",");
          buffer.append(writeValue(entry.getKey(), iFormat));
          buffer.append(":");
          buffer.append(writeValue(entry.getValue(), iFormat));
        }
      }
      buffer.append('}');
      return buffer.toString();
    } catch (IOException e) {
      throw new OSerializationException("Error on serializing map", e);
    }
  }

  public OJSONWriter beginObject() throws IOException {
    beginObject(0, false, null);
    return this;
  }

  public OJSONWriter beginObject(final int iIdentLevel) throws IOException {
    beginObject(iIdentLevel, false, null);
    return this;
  }

  public OJSONWriter beginObject(final Object iName) throws IOException {
    beginObject(-1, false, iName);
    return this;
  }

  public OJSONWriter beginObject(final int iIdentLevel, final boolean iNewLine, final Object iName) throws IOException {
    if (!firstAttribute)
      out.append(",");

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append("\"" + iName.toString() + "\":");
      if (prettyPrint)
        out.append(' ');
    }

    out.append('{');

    firstAttribute = true;
    return this;
  }

  public OJSONWriter writeRecord(final int iIdentLevel, final boolean iNewLine, final Object iName, final ORecord iRecord) throws IOException {
    if (!firstAttribute)
      out.append(",");

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append("\"" + iName.toString() + "\":");
      if (prettyPrint)
        out.append(' ');
    }

    out.append(iRecord.toJSON(format));

    firstAttribute = false;
    return this;
  }

  public OJSONWriter endObject() throws IOException {
    format(-1, true);
    out.append('}');
    return this;
  }

  public OJSONWriter endObject(final int iIdentLevel) throws IOException {
    return endObject(iIdentLevel, true);
  }

  public OJSONWriter endObject(final int iIdentLevel, final boolean iNewLine) throws IOException {
    format(iIdentLevel, iNewLine);
    out.append('}');
    firstAttribute = false;
    return this;
  }

  public OJSONWriter beginCollection(final String iName) throws IOException {
    return beginCollection(-1, false, iName);
  }

  public OJSONWriter beginCollection(final int iIdentLevel, final boolean iNewLine, final String iName) throws IOException {
    if (!firstAttribute)
      out.append(",");

    format(iIdentLevel, iNewLine);

    if (iName != null && !iName.isEmpty()) {
      out.append(writeValue(iName, format));
      out.append(":");
      if (prettyPrint)
        out.append(' ');
    }
    out.append("[");

    firstAttribute = true;
    return this;
  }

  public OJSONWriter endCollection() throws IOException {
    return endCollection(-1, false);
  }

  public OJSONWriter endCollection(final int iIdentLevel, final boolean iNewLine) throws IOException {
    format(iIdentLevel, iNewLine);
    firstAttribute = false;
    out.append(']');
    return this;
  }

  public OJSONWriter writeObjects(final String iName, Object[]... iPairs) throws IOException {
    return writeObjects(-1, false, iName, iPairs);
  }

  public OJSONWriter writeObjects(int iIdentLevel, boolean iNewLine, final String iName, Object[]... iPairs) throws IOException {
    for (int i = 0; i<iPairs.length; ++i) {
      beginObject(iIdentLevel, true, iName);
      for (int k = 0; k<iPairs[i].length; ) {
        writeAttribute(iIdentLevel + 1, false, (String) iPairs[i][k++], iPairs[i][k++], format);
      }
      endObject(iIdentLevel, false);
    }
    return this;
  }

  public OJSONWriter writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue) throws IOException {
    return writeAttribute(iIdentLevel, iNewLine, iName, iValue, format);
  }

  public OJSONWriter writeAttribute(final String iName, final Object iValue) throws IOException {
    return writeAttribute(-1, false, iName, iValue, format);
  }

  public OJSONWriter writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue, final String iFormat) throws IOException {
    if (!firstAttribute)
      out.append(",");

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append(writeValue(iName, iFormat));
      out.append(":");
      if (prettyPrint)
        out.append(' ');
    }

    if (iFormat != null && iFormat.contains("graph") && iName != null && (iName.startsWith("in_") || iName.startsWith("out_")) && (iValue == null || iValue instanceof OIdentifiable)) {
      // FORCE THE OUTPUT AS COLLECTION
      out.append('[');
      if (iValue instanceof OIdentifiable) {
        final boolean shallow = iFormat != null && iFormat.contains("shallow");
        if (shallow)
          out.append("1");
        else
          out.append(writeValue(iValue, iFormat));
      }
      out.append(']');
    } else
      out.append(writeValue(iValue, iFormat, iIdentLevel));

    firstAttribute = false;
    return this;
  }

  public OJSONWriter writeValue(final int iIdentLevel, final boolean iNewLine, final Object iValue) throws IOException {
    if (!firstAttribute)
      out.append(",");

    format(iIdentLevel, iNewLine);

    out.append(writeValue(iValue, format));

    firstAttribute = false;
    return this;
  }

  public OJSONWriter flush() throws IOException {
    out.flush();
    return this;
  }

  public OJSONWriter close() throws IOException {
    out.close();
    return this;
  }

  public OJSONWriter append(final String iText) throws IOException {
    out.append(iText);
    return this;
  }

  public boolean isPrettyPrint() {
    return prettyPrint;
  }

  public OJSONWriter setPrettyPrint(boolean prettyPrint) {
    this.prettyPrint = prettyPrint;
    return this;
  }

  public void write(final String iText) throws IOException {
    out.append(iText);
  }

  public void newline() throws IOException {
    if (prettyPrint)
      out.append("\r\n");
  }

  public void resetAttributes() {
    firstAttribute = true;
  }

  private OJSONWriter format(final int iIdentLevel, final boolean iNewLine) throws IOException {
    if (iIdentLevel>-1) {
      if (iNewLine)
        newline();

      if (prettyPrint)
        for (int i = 0; i<iIdentLevel; ++i)
          out.append("  ");
    }

    return this;
  }
}
