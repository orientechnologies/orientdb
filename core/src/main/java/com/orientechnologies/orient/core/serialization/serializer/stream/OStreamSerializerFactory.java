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

import com.orientechnologies.orient.core.exception.OConfigurationException;

public class OStreamSerializerFactory {
	public static OStreamSerializer get(final String iName) {
		try {
			if (iName.equals(OStreamSerializerRecord.NAME))
				return OStreamSerializerRecord.INSTANCE;

			else if (iName.equals(OStreamSerializerString.NAME))
				return OStreamSerializerString.INSTANCE;

			else if (iName.equals(OStreamSerializerLong.NAME))
				return OStreamSerializerLong.INSTANCE;

			else if (iName.equals(OStreamSerializerLiteral.NAME))
				return OStreamSerializerLiteral.INSTANCE;

			else if (iName.equals(OStreamSerializerAnyRecord.NAME))
				return OStreamSerializerAnyRecord.INSTANCE;

			else if (iName.equals(OStreamSerializerAnyStreamable.NAME))
				return OStreamSerializerAnyStreamable.INSTANCE;

			else if (iName.equals(OStreamSerializerAnyStatic.NAME))
				return new OStreamSerializerAnyStatic(null);

			else if (iName.equals(OStreamSerializerRID.NAME))
				return OStreamSerializerRID.INSTANCE;

			else if (iName.equals(OStreamSerializerListRID.NAME))
				return OStreamSerializerListRID.INSTANCE;

			throw new OConfigurationException("Stream Serializer '" + iName + "' not registered");

		} catch (Exception e) {
			throw new OConfigurationException("Error on the retrieving of Stream Serializer '" + iName + "'", e);
		}
	}
}
