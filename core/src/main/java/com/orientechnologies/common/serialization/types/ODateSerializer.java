/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.serialization.types;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

/**
 * Serializer for {@link Date} type, it serializes it without time part.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class ODateSerializer implements OBinarySerializer<Date> {

  public static final byte ID = 4;
  public static final ODateSerializer INSTANCE = new ODateSerializer();

  public int getObjectSize(Date object, Object... hints) {
    return OLongSerializer.LONG_SIZE;
  }

  public void serialize(Date object, byte[] stream, int startPosition, Object... hints) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    dateTimeSerializer.serialize(calendar.getTime(), stream, startPosition);
  }

  public Date deserialize(byte[] stream, int startPosition) {
    ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserialize(stream, startPosition);
  }

  public int getObjectSize(byte[] stream, int startPosition) {
    return OLongSerializer.LONG_SIZE;
  }

  public byte getId() {
    return ID;
  }

  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OLongSerializer.LONG_SIZE;
  }

  public void serializeNativeObject(
      final Date object, byte[] stream, int startPosition, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    final ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    dateTimeSerializer.serializeNativeObject(calendar.getTime(), stream, startPosition);
  }

  public Date deserializeNativeObject(byte[] stream, int startPosition) {
    ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeNativeObject(stream, startPosition);
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public Date preprocess(Date value, Object... hints) {
    if (value == null) {
      return null;
    }
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(value);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    return calendar.getTime();
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Date object, ByteBuffer buffer, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    final ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    dateTimeSerializer.serializeInByteBufferObject(calendar.getTime(), buffer);
  }

  /** {@inheritDoc} */
  @Override
  public Date deserializeFromByteBufferObject(ByteBuffer buffer) {
    final ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeFromByteBufferObject(buffer);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return OLongSerializer.LONG_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public Date deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final ODateTimeSerializer dateTimeSerializer = ODateTimeSerializer.INSTANCE;
    return dateTimeSerializer.deserializeFromByteBufferObject(buffer, walChanges, offset);
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OLongSerializer.LONG_SIZE;
  }
}
