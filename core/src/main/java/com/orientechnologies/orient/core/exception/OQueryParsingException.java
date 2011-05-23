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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OException;

public class OQueryParsingException extends OException {

	private String						text;
	private int								position					= -1;
	private static final long	serialVersionUID	= -7430575036316163711L;

	public OQueryParsingException(final String iMessage) {
		super(iMessage);
	}

	public OQueryParsingException(final String iMessage, final Throwable cause) {
		super(iMessage, cause);
	}

	public OQueryParsingException(final String iMessage, final String iText, final int iPosition, final Throwable cause) {
		super(iMessage, cause);
		text = iText;
		position = iPosition;
	}

	public OQueryParsingException(final String iMessage, final String iText, final int iPosition) {
		super(iMessage);
		text = iText;
		position = iPosition;
	}

	@Override
	public String getMessage() {
		StringBuilder buffer = new StringBuilder();
		if (position > -1) {
			buffer.append("Error on parsing query at position #");
			buffer.append(position);
			buffer.append(": ");
		}

		buffer.append(super.getMessage());

		if (text != null) {
			buffer.append("\nQuery: ");
			buffer.append(text);
			buffer.append("\n------");
			for (int i = 0; i < position - 1; ++i)
				buffer.append("-");

			buffer.append("^");
		}
		return buffer.toString();
	}

	public String getText() {
		return text;
	}

}
