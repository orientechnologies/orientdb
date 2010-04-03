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
package com.orientechnologies.utility.impexp;

import java.io.IOException;
import java.io.Writer;

public class OJSONWriter {
	private Writer	out;
	private boolean	prettyPrint			= true;
	private boolean	firstAttribute	= true;

	public OJSONWriter(Writer out) {
		this.out = out;
	}

	public void beginSection(final int iIdentLevel, final boolean iNewLine, final Object iName) throws IOException {
		if (!firstAttribute)
			out.append(", ");

		if (iNewLine)
			out.append("\n");

		if (prettyPrint)
			for (int i = 0; i < iIdentLevel; ++i)
				out.append("  ");

		out.append(iName != null ? iName.toString() : "null");
		out.append("{");
		firstAttribute = true;
	}

	public void endSection(final int iIdentLevel, final boolean iNewLine) throws IOException {
		if (iNewLine)
			out.append("\n");

		if (prettyPrint)
			for (int i = 0; i < iIdentLevel; ++i)
				out.append("  ");

		out.append("}");
	}

	public void writeAttribute(final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue)
			throws IOException {
		if (!firstAttribute)
			out.append(", ");

		if (iNewLine)
			out.append("\n");

		if (prettyPrint)
			for (int i = 0; i < iIdentLevel; ++i)
				out.append("  ");

		out.append(iName);
		out.append(":");
		if (iValue == null) {
			out.append("null");
		} else {
			out.append("'");
			out.append(iValue.toString());
			out.append("'");
		}
		firstAttribute = false;
	}

	public void writeLine(final String iValue) throws IOException {
		out.append(iValue);
		out.append("\n");
	}

	public void flush() throws IOException {
		out.flush();
	}

	public void close() throws IOException {
		out.close();
	}
}
