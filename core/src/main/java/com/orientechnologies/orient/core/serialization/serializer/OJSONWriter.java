/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.serialization.serializer;

import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

@SuppressWarnings("unchecked")
public class OJSONWriter {
	private static final String	DEF_FORMAT			= "rid,type,version,class,attribSameRow,indent:6";
	private Writer							out;
	private boolean							prettyPrint			= true;
	private boolean							firstAttribute	= true;
	private final String				format;

	public OJSONWriter(final Writer out, final String iJsonFormat) {
		this.out = out;
		format = iJsonFormat;
	}

	public OJSONWriter(final Writer out) {
		this.out = out;
		this.format = DEF_FORMAT;
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
		beginObject(0, false, iName);
		return this;
	}

	public OJSONWriter beginObject(final int iIdentLevel, final boolean iNewLine, final Object iName) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		if (iName != null)
			out.append("\"" + iName.toString() + "\":");

		out.append('{');

		firstAttribute = true;
		return this;
	}

	public OJSONWriter endObject() throws IOException {
		format(0, true);
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

	public OJSONWriter beginCollection(final int iIdentLevel, final boolean iNewLine, final String iName) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iName));
		out.append(": [");

		firstAttribute = true;
		return this;
	}

	public OJSONWriter endCollection(final int iIdentLevel, final boolean iNewLine) throws IOException {
		format(iIdentLevel, iNewLine);
		firstAttribute = false;
		out.append(']');
		return this;
	}

	public void writeObjects(int iIdentLevel, boolean iNewLine, final String iName, Object[]... iPairs) throws IOException {
		for (int i = 0; i < iPairs.length; ++i) {
			beginObject(iIdentLevel, true, iName);
			for (int k = 0; k < iPairs[i].length;) {
				writeAttribute(iIdentLevel + 1, false, (String) iPairs[i][k++], iPairs[i][k++], format);
			}
			endObject(iIdentLevel, false);
		}
	}

	public OJSONWriter writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue)
			throws IOException {
		return writeAttribute(iIdentLevel, iNewLine, iName, iValue, format);
	}

	public OJSONWriter writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue,
			final String iFormat) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iName, iFormat));
		out.append(": ");
		out.append(writeValue(iValue, iFormat));

		firstAttribute = false;
		return this;
	}

	public OJSONWriter writeValue(final int iIdentLevel, final boolean iNewLine, final Object iValue) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		format(iIdentLevel, iNewLine);

		out.append(writeValue(iValue, format));

		firstAttribute = false;
		return this;
	}

	public static String writeValue(final Object iValue) throws IOException {
		return writeValue(iValue, DEF_FORMAT);
	}

	public static String writeValue(final Object iValue, final String iFormat) throws IOException {
		final StringBuilder buffer = new StringBuilder();

		final boolean oldAutoConvertSettings;

		if (iValue instanceof ORecordLazyMultiValue) {
			oldAutoConvertSettings = ((ORecordLazyMultiValue) iValue).isAutoConvertToRecord();
			((ORecordLazyMultiValue) iValue).setAutoConvertToRecord(false);
		} else
			oldAutoConvertSettings = false;

		if (iValue == null)
			buffer.append("null");

		else if (iValue instanceof ORecordId) {
			final ORecordId rid = (ORecordId) iValue;
			buffer.append('\"');
			rid.toString(buffer);
			buffer.append('\"');

		} else if (iValue instanceof ORecord<?>) {
			final ORecord<?> linked = (ORecord<?>) iValue;
			if (linked.getIdentity().isValid()) {
				buffer.append('\"');
				linked.getIdentity().toString(buffer);
				buffer.append('\"');
			} else {
				buffer.append(linked.toJSON(iFormat));
			}

		} else if (iValue.getClass().isArray()) {

			if (iValue instanceof byte[]) {
				buffer.append('\"');
				final byte[] source = (byte[]) iValue;

				buffer.append(OBase64Utils.encodeBytes(source));

				buffer.append('\"');
			} else {
				buffer.append('[');
				for (int i = 0; i < Array.getLength(iValue); ++i) {
					if (i > 0)
						buffer.append(", ");
					buffer.append(writeValue(Array.get(iValue, i), iFormat));
				}
				buffer.append(']');
			}

		} else if (iValue instanceof Collection<?>) {
			final Collection<Object> coll = (Collection<Object>) iValue;
			buffer.append('[');
			int i = 0;
			for (Iterator<Object> it = coll.iterator(); it.hasNext(); ++i) {
				if (i > 0)
					buffer.append(", ");
				buffer.append(writeValue(it.next(), iFormat));
			}
			buffer.append(']');

		} else if (iValue instanceof Map<?, ?>) {
			final Map<Object, Object> map = (Map<Object, Object>) iValue;
			buffer.append('{');
			int i = 0;
			Entry<Object, Object> entry;
			for (Iterator<Entry<Object, Object>> it = map.entrySet().iterator(); it.hasNext(); ++i) {
				entry = it.next();
				if (i > 0)
					buffer.append(", ");
				buffer.append(writeValue(entry.getKey(), iFormat));
				buffer.append(": ");
				buffer.append(writeValue(entry.getValue(), iFormat));
			}
			buffer.append('}');

		} else if (iValue instanceof Date) {
			final SimpleDateFormat dateFormat = new SimpleDateFormat(ORecordSerializerJSON.DEF_DATE_FORMAT);
			buffer.append('"');
			buffer.append(dateFormat.format(iValue));
			buffer.append('"');
		} else if (iValue instanceof String) {
			final String v = (String) iValue;
			if (v.startsWith("\""))
				buffer.append(v);
			else {
				buffer.append('"');
				buffer.append(v);
				buffer.append('"');
			}
		} else
			buffer.append(iValue.toString());

		if (iValue instanceof ORecordLazyMultiValue)
			((ORecordLazyMultiValue) iValue).setAutoConvertToRecord(oldAutoConvertSettings);

		return buffer.toString();
	}

	public OJSONWriter flush() throws IOException {
		out.flush();
		return this;
	}

	public OJSONWriter close() throws IOException {
		out.close();
		return this;
	}

	private OJSONWriter format(final int iIdentLevel, final boolean iNewLine) throws IOException {
		if (iNewLine) {
			out.append('\n');

			if (prettyPrint)
				for (int i = 0; i < iIdentLevel; ++i)
					out.append("  ");
		}
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

	public static Object encode(final Object iValue) {
		if (iValue instanceof String) {
			return OStringSerializerHelper.java2unicode(((String) iValue).replace("\\", "\\\\").replace("\"", "\\\""));
		} else
			return iValue;
	}
}
