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
package com.orientechnologies.orient.core.serialization.serializer.string;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerHelper;

public class OStringSerializerAnyRuntime implements OStringSerializer {
	public static final OStringSerializerAnyRuntime	INSTANCE	= new OStringSerializerAnyRuntime();
	private static final String											NAME			= "au";

	public String getName() {
		return NAME;
	}

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(final ODatabaseComplex<?> iDatabase, final String iStream) {
		if (iStream == null || iStream.length() == 0)
			// NULL VALUE
			return null;

		int pos = iStream.indexOf(OStreamSerializerHelper.SEPARATOR);
		if (pos < 0)
			OLogManager.instance().error(this, "Class signature not found in ANY element: " + iStream, OSerializationException.class);

		final String className = iStream.substring(0, pos);

		try {
			Class<?> clazz = Class.forName(className);
			return clazz.getDeclaredConstructor(String.class).newInstance(iStream.substring(pos + 1));
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling content. Class: " + className, e, OSerializationException.class);
		}
		return null;
	}

	public StringBuilder toStream(final ODatabaseComplex<?> iDatabase, final StringBuilder iOutput, Object iObject) {
		if (iObject != null) {
			iOutput.append(iObject.getClass().getName());
			iOutput.append(OStreamSerializerHelper.SEPARATOR);
			iOutput.append(iObject.toString());
		}

		return iOutput;
	}
}
