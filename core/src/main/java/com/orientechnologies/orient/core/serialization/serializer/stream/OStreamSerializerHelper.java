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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.entity.OClassDictionary;
import com.orientechnologies.orient.core.exception.OSerializationException;

/**
 * Abstract class. Allows short and long form. Examples: <br/>
 * Short form: @[type][RID] where type = 1 byte<br/>
 * Long form: org.myapp.Myrecord|[RID]<br/>
 * 
 * @author Luca Garulli
 * 
 */
public class OStreamSerializerHelper {

	public static final String	SEPARATOR					= "|";
	private static final char			SHORT_FORM_PREFIX	= '!';

	public static StringBuilder writeRecordType(final Class<?> cls, final StringBuilder iBuffer) {
		// SEARCH INTO THE SERIALIZER REGISTER IF THE IMPLEMENTATION WAS REGISTERED TO GET THE SHORT FORM (AND OPTIMIZING IN SIZE AND
		// WRITE TIMES)
		Character c = OClassDictionary.instance().getCodeByClass(cls);
		if (c != null) {
			// FOUND: WRITE THE SHORT FORM
			iBuffer.append(SHORT_FORM_PREFIX);
			iBuffer.append(c);
		} else {
			// NOT FOUND: PROBABLY A CUSTOM IMPL: WRITE THE FULL CLASS NAME
			iBuffer.append(cls.getName());
			iBuffer.append(SEPARATOR);
		}
		return iBuffer;
	}

	public static Class<?> readRecordType(final String iBuffer, final StringBuilder iContent) throws ClassNotFoundException {
		Class<?> cls;
		final int pos;
		if (iBuffer.charAt(0) == SHORT_FORM_PREFIX) {
			// SHORT FORM
			cls = OClassDictionary.instance().getClassByCode(iBuffer.charAt(1));
			pos = 1;
		} else {
			// LONG FORM
			pos = iBuffer.indexOf(SEPARATOR);
			if (pos < 0)
				OLogManager.instance().error(null, "Class signature not found in the buffer: " + iBuffer, OSerializationException.class);
			final String className = iBuffer.substring(0, pos);
			cls = Class.forName(className);
		}

		// SET THE CONTENT
		iContent.append(iBuffer.substring(pos + 1));

		return cls;
	}
}
