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
 * Serializer for {@link Date} type.
 *
 * @author Ilya Bershadskiy (ibersh20-at-gmail.com)
 * @since 20.01.12
 */
public class ODateTimeSerializer implements OBinarySerializer<Date> {
  public static final byte ID = 5;
  public static final ODateTimeSerializer INSTANCE = new ODateTimeSerializer();

  public int getObjectSize(Date object, Object... hints) {
    return OLongSerializer.LONG_SIZE;
  }

  public void serialize(Date object, byte[] stream, int startPosition, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    OLongSerializer.INSTANCE.serializeLiteral(calendar.getTimeInMillis(), stream, startPosition);
  }

  public Date deserialize(byte[] stream, int startPosition) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(OLongSerializer.INSTANCE.deserializeLiteral(stream, startPosition));
    return calendar.getTime();
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

  @Override
  public void serializeNativeObject(
      Date object, byte[] stream, int startPosition, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    OLongSerializer.INSTANCE.serializeNative(calendar.getTimeInMillis(), stream, startPosition);
  }

  @Override
  public Date deserializeNativeObject(byte[] stream, int startPosition) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(OLongSerializer.INSTANCE.deserializeNative(stream, startPosition));
    return calendar.getTime();
  }

  public boolean isFixedLength() {
    return true;
  }

  public int getFixedLength() {
    return OLongSerializer.LONG_SIZE;
  }

  @Override
  public Date preprocess(Date value, Object... hints) {
    return value;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(Date object, ByteBuffer buffer, Object... hints) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTime(object);
    buffer.putLong(calendar.getTimeInMillis());
  }

  /** {@inheritDoc} */
  @Override
  public Date deserializeFromByteBufferObject(ByteBuffer buffer) {
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(buffer.getLong());
    return calendar.getTime();
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
    final Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(walChanges.getLongValue(buffer, offset));
    return calendar.getTime();
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return OLongSerializer.LONG_SIZE;
  }
}
