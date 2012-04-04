/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Calendar;
import java.util.Date;

import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

/**
 * Serializer for  {@link com.orientechnologies.orient.core.metadata.schema.OType#DATETIME}
 *
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class ODateTimeSerializer implements OBinarySerializer<Date> {
	public static ODateTimeSerializer INSTANCE = new ODateTimeSerializer();
	public static final byte ID = 5;

	public int getObjectSize(Date object) {
		return OLongSerializer.LONG_SIZE;
	}

	public void serialize(Date object, byte[] stream, int startPosition) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(object);
		OLongSerializer longSerializer = OLongSerializer.INSTANCE;
		longSerializer.serialize(calendar.getTimeInMillis(), stream, startPosition);
	}

	public Date deserialize(byte[] stream, int startPosition) {
		Calendar calendar = Calendar.getInstance();
		OLongSerializer longSerializer = OLongSerializer.INSTANCE;
		calendar.setTimeInMillis(longSerializer.deserialize(stream, startPosition));
		return calendar.getTime();
	}

	public int getObjectSize(byte[] stream, int startPosition) {
		return OLongSerializer.LONG_SIZE;
	}

	public byte getId() {
		return ID;
	}

}
