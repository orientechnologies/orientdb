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

package com.orientechnologies.orient.core.serialization.serializer.binary;

/**
 * This interface is used for serializing OrientDB datatypes in binary format.
 * Serialized content is written into buffer that will contain not only given object presentation but
 * all binary content. Such approach prevents creation of separate byte array for each object
 * and decreased GC overhead.
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public interface OBinarySerializer<T> {

	/**
	 * Obtain size of the serialized object
	 * Size is the amount of bites that required for storing object (for example: for storing integer we need 4 bytes)
	 *
	 * @param object is the object to measure its size
	 * @return size of the serialized object
	 */
	int getObjectSize(T object);

	/**
	 * Return size serialized presentation of given object.
	 *
	 * @param stream          Serialized content.
	 * @param startPosition   Position from which serialized presentation of given object is stored.
	 * @return                Size serialized presentation of given object in bytes.
	 */
	int getObjectSize(byte[] stream, int startPosition);

	/**
	 * Writes object to the stream starting from the startPosition
	 *
	 * @param object is the object to serialize
	 * @param stream is the stream where object will be written
	 * @param startPosition is the position to start writing from
	 */
	void serialize(T object, byte[] stream, int startPosition);

	/**
	 * Reads object from the stream starting from the startPosition
	 *
	 * @param stream is the stream from object will be read
	 * @param startPosition is the position to start reading from
	 * @return instance of the deserialized object
	 */
	T deserialize(byte[] stream, int startPosition);

	/**
	 * @return Identifier of given serializer.
	 */
	byte getId();
}
