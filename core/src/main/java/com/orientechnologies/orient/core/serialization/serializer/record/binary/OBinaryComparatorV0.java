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

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readOptimizedLink;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.orientechnologies.orient.core.serialization.serializer.record.binary.HelperClasses.stringFromBytes;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Implementation v0 of comparator based on protocol v0.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OBinaryComparatorV0 implements OBinaryComparator {

  public OBinaryComparatorV0() {}

  public boolean isBinaryComparable(final OType iType) {
    switch (iType) {
      case INTEGER:
      case LONG:
      case DATETIME:
      case SHORT:
      case STRING:
      case DOUBLE:
      case FLOAT:
      case BYTE:
      case BOOLEAN:
      case DATE:
      case BINARY:
      case LINK:
      case DECIMAL:
        return true;
      default:
        return false;
    }
  }

  /**
   * Compares if 2 field values are the same.
   *
   * @param iField1 First value to compare
   * @param iField2 Second value to compare
   * @return true if they match, otherwise false
   */
  @Override
  public boolean isEqual(final OBinaryField iField1, final OBinaryField iField2) {
    final BytesContainer fieldValue1 = iField1.bytes;
    final int offset1 = fieldValue1.offset;

    final BytesContainer fieldValue2 = iField2.bytes;
    final int offset2 = fieldValue2.offset;

    try {
      switch (iField1.type) {
        case INTEGER:
          {
            final int value1 = OVarIntSerializer.readAsInteger(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case DATE:
                {
                  final long value2 =
                      (OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  return Integer.parseInt(readString(fieldValue2)) == value1;
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.intValue();
                }
            }
            break;
          }

        case LONG:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case DATE:
                {
                  final long value2 =
                      (OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  return Long.parseLong(readString(fieldValue2)) == value1;
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.longValue();
                }
            }
            break;
          }

        case SHORT:
          {
            final short value1 = OVarIntSerializer.readAsShort(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case DATE:
                {
                  final long value2 =
                      (OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  return Short.parseShort(readString(fieldValue2)) == value1;
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.shortValue();
                }
            }
            break;
          }

        case STRING:
          {
            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return Integer.parseInt(readString(fieldValue1)) == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return Long.parseLong(readString(fieldValue1)) == value2;
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return Long.parseLong(readString(fieldValue1)) == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return Short.parseShort(readString(fieldValue1)) == value2;
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return Byte.parseByte(readString(fieldValue1)) == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return Float.parseFloat(readString(fieldValue1)) == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return Double.parseDouble(readString(fieldValue1)) == value2;
                }
              case STRING:
                {
                  final int len1 = OVarIntSerializer.readAsInteger(fieldValue1);
                  final int len2 = OVarIntSerializer.readAsInteger(fieldValue2);

                  if (len1 != len2) return false;

                  final OCollate collate;
                  if (iField1.collate != null
                      && !ODefaultCollate.NAME.equals(iField1.collate.getName())) {
                    collate = iField1.collate;
                  } else if (iField2.collate != null
                      && !ODefaultCollate.NAME.equals(iField2.collate.getName())) {
                    collate = iField2.collate;
                  } else {
                    collate = null;
                  }

                  if (collate != null) {
                    final String str1 =
                        (String)
                            collate.transform(
                                stringFromBytes(fieldValue1.bytes, fieldValue1.offset, len1));
                    final String str2 =
                        (String)
                            collate.transform(
                                stringFromBytes(fieldValue2.bytes, fieldValue2.offset, len2));

                    return str1.equals(str2);

                  } else {
                    for (int i = 0; i < len1; ++i) {
                      if (fieldValue1.bytes[fieldValue1.offset + i]
                          != fieldValue2.bytes[fieldValue2.offset + i]) return false;
                    }
                  }
                  return true;
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return new BigDecimal(readString(fieldValue1)).equals(value2);
                }
              case BOOLEAN:
                {
                  final boolean value2 = readByte(fieldValue2) == 1;
                  return Boolean.parseBoolean(readString(fieldValue1)) == value2;
                }
            }
            break;
          }

        case DOUBLE:
          {
            final long value1AsLong = readLong(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2AsLong = readLong(fieldValue2);
                  return value1AsLong == value2AsLong;
                }
              case STRING:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  return Double.parseDouble(readString(fieldValue2)) == value1;
                }
              case DECIMAL:
                {
                  final double value1 = Double.longBitsToDouble(value1AsLong);
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.doubleValue();
                }
            }
            break;
          }

        case FLOAT:
          {
            final int value1AsInt = readInteger(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2AsInt = readInteger(fieldValue2);
                  return value1AsInt == value2AsInt;
                }
              case DOUBLE:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  return Float.parseFloat(readString(fieldValue2)) == value1;
                }
              case DECIMAL:
                {
                  final float value1 = Float.intBitsToFloat(value1AsInt);
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.floatValue();
                }
            }
            break;
          }

        case BYTE:
          {
            final byte value1 = readByte(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  final byte value2 = Byte.parseByte((readString(fieldValue2)));
                  return value1 == value2;
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.byteValue();
                }
            }
            break;
          }

        case BOOLEAN:
          {
            final boolean value1 = readByte(fieldValue1) == 1;

            switch (iField2.type) {
              case BOOLEAN:
                {
                  final boolean value2 = readByte(fieldValue2) == 1;
                  return value1 == value2;
                }
              case STRING:
                {
                  final String str = readString(fieldValue2);
                  return Boolean.parseBoolean(str) == value1;
                }
            }
            break;
          }

        case DATE:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1) * MILLISEC_PER_DAY;

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  value2 =
                      convertDayToTimezone(
                          ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), value2);
                  return value1 == value2;
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.longValue();
                }
            }
            break;
          }

        case DATETIME:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1 == value2;
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1 == value2;
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return value1 == value2;
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1 == value2;
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1 == value2;
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1 == value2;
                }
              case STRING:
                {
                  final String value2AsString = readString(fieldValue2);

                  if (OIOUtils.isLong(value2AsString)) {
                    final long value2 = Long.parseLong(value2AsString);
                    return value1 == value2;
                  }

                  final ODatabaseDocumentInternal db =
                      ODatabaseRecordThreadLocal.instance().getIfDefined();
                  try {
                    final DateFormat dateFormat;
                    if (db != null) {
                      dateFormat = ODateHelper.getDateTimeFormatInstance(db);
                    } else {
                      dateFormat =
                          new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
                    }

                    final Date value2AsDate = dateFormat.parse(value2AsString);
                    final long value2 = value2AsDate.getTime();
                    return value1 == value2;
                  } catch (ParseException ignore) {
                    try {
                      final SimpleDateFormat dateFormat;
                      if (db != null) {
                        dateFormat = db.getStorageInfo().getConfiguration().getDateFormatInstance();
                      } else {
                        dateFormat =
                            new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATE_FORMAT);
                      }

                      final Date value2AsDate = dateFormat.parse(value2AsString);
                      final long value2 = value2AsDate.getTime();
                      return value1 == value2;
                    } catch (ParseException ignored) {
                      return new Date(value1).toString().equals(value2AsString);
                    }
                  }
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1 == value2.longValue();
                }
            }
            break;
          }

        case BINARY:
          {
            switch (iField2.type) {
              case BINARY:
                {
                  final int length1 = OVarIntSerializer.readAsInteger(fieldValue1);
                  final int length2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  if (length1 != length2) return false;

                  for (int i = 0; i < length1; ++i) {
                    if (fieldValue1.bytes[fieldValue1.offset + i]
                        != fieldValue2.bytes[fieldValue2.offset + i]) return false;
                  }
                  return true;
                }
            }
            break;
          }

        case LINK:
          {
            switch (iField2.type) {
              case LINK:
                {
                  final int clusterId1 = OVarIntSerializer.readAsInteger(fieldValue1);
                  final int clusterId2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  if (clusterId1 != clusterId2) return false;

                  final long clusterPos1 = OVarIntSerializer.readAsLong(fieldValue1);
                  final long clusterPos2 = OVarIntSerializer.readAsLong(fieldValue2);
                  if (clusterPos1 == clusterPos2) return true;
                  break;
                }
              case STRING:
                {
                  return readOptimizedLink(fieldValue1, false)
                      .toString()
                      .equals(readString(fieldValue2));
                }
            }
            break;
          }

        case DECIMAL:
          {
            final BigDecimal value1 =
                ODecimalSerializer.INSTANCE.deserialize(fieldValue1.bytes, fieldValue1.offset);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1.equals(new BigDecimal(value2));
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1.equals(new BigDecimal(value2));
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1.equals(new BigDecimal(value2));
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1.equals(new BigDecimal(value2));
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1.equals(new BigDecimal(value2));
                }
              case STRING:
                {
                  return value1.toString().equals(readString(fieldValue2));
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1.equals(value2);
                }
            }
            break;
          }
      }
    } finally {
      fieldValue1.offset = offset1;
      fieldValue2.offset = offset2;
    }

    return false;
  }

  /**
   * Compares two values executing also conversion between types.
   *
   * @param iField1 First value to compare
   * @param iField2 Second value to compare
   * @return 0 if they matches, >0 if first value is major than second, <0 in case is minor
   */
  @Override
  public int compare(final OBinaryField iField1, final OBinaryField iField2) {
    final BytesContainer fieldValue1 = iField1.bytes;
    final int offset1 = fieldValue1.offset;

    final BytesContainer fieldValue2 = iField2.bytes;
    final int offset2 = fieldValue2.offset;

    try {
      switch (iField1.type) {
        case INTEGER:
          {
            final int value1 = OVarIntSerializer.readAsInteger(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Integer.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final int value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .intValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case LONG:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Long.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final long value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .longValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case SHORT:
          {
            final short value1 = OVarIntSerializer.readAsShort(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Short.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final short value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .shortValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case STRING:
          {
            final String value1 = readString(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1.compareTo(Integer.toString(value2));
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1.compareTo(Long.toString(value2));
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return value1.compareTo(Long.toString(value2));
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1.compareTo(Short.toString(value2));
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1.compareTo(Byte.toString(value2));
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1.compareTo(Float.toString(value2));
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1.compareTo(Double.toString(value2));
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);

                  final OCollate collate;
                  if (iField1.collate != null
                      && !ODefaultCollate.NAME.equals(iField1.collate.getName())) {
                    collate = iField1.collate;
                  } else if (iField2.collate != null
                      && !ODefaultCollate.NAME.equals(iField2.collate.getName())) {
                    collate = iField2.collate;
                  } else {
                    collate = null;
                  }

                  if (collate != null) {
                    final String str1 = (String) collate.transform(value1);
                    final String str2 = (String) collate.transform(value2);
                    return str1.compareTo(str2);
                  }

                  return value1.compareTo(value2);
                }
              case BOOLEAN:
                {
                  final boolean value2 = readByte(fieldValue2) == 1;
                  return value1.compareTo(Boolean.toString(value2));
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return new BigDecimal(value1).compareTo(value2);
                }
            }
            break;
          }

        case DOUBLE:
          {
            final double value1 = Double.longBitsToDouble(readLong(fieldValue1));

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Double.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final double value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .doubleValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case FLOAT:
          {
            final float value1 = Float.intBitsToFloat(readInteger(fieldValue1));

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Float.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final String value2 = readString(fieldValue2);
                  return Float.toString(value1).compareTo(value2);
                }
            }
            break;
          }

        case BYTE:
          {
            final byte value1 = readByte(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return Byte.toString(value1).compareTo(value2);
                }
              case DECIMAL:
                {
                  final byte value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .byteValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case BOOLEAN:
          {
            final boolean value1 = readByte(fieldValue1) == 1;

            switch (iField2.type) {
              case BOOLEAN:
                {
                  final boolean value2 = readByte(fieldValue2) == 1;
                  return (value1 == value2) ? 0 : value1 ? 1 : -1;
                }
              case STRING:
                {
                  final boolean value2 = Boolean.parseBoolean(readString(fieldValue2));
                  return (value1 == value2) ? 0 : value1 ? 1 : -1;
                }
            }
            break;
          }

        case DATETIME:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2AsString = readString(fieldValue2);

                  if (OIOUtils.isLong(value2AsString)) {
                    final long value2 = Long.parseLong(value2AsString);
                    return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                  }
                  try {
                    final DateFormat dateFormat = ODateHelper.getDateTimeFormatInstance();
                    final Date value2AsDate = dateFormat.parse(value2AsString);
                    final long value2 = value2AsDate.getTime();
                    return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                  } catch (ParseException ignored) {
                    try {
                      DateFormat dateFormat = ODateHelper.getDateFormatInstance();

                      final Date value2AsDate = dateFormat.parse(value2AsString);
                      final long value2 = value2AsDate.getTime();
                      return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                    } catch (ParseException ignore) {
                      return new Date(value1).toString().compareTo(value2AsString);
                    }
                  }
                }
              case DECIMAL:
                {
                  final long value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .longValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case DATE:
          {
            final long value1 = OVarIntSerializer.readAsLong(fieldValue1) * MILLISEC_PER_DAY;

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DATE:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2) * MILLISEC_PER_DAY;
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
              case STRING:
                {
                  final String value2AsString = readString(fieldValue2);

                  if (OIOUtils.isLong(value2AsString)) {
                    final long value2 = Long.parseLong(value2AsString);
                    return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                  }

                  final ODatabaseDocumentInternal db =
                      ODatabaseRecordThreadLocal.instance().getIfDefined();
                  try {
                    final DateFormat dateFormat;
                    if (db != null) {
                      dateFormat = ODateHelper.getDateFormatInstance(db);
                    } else {
                      dateFormat = new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATE_FORMAT);
                    }
                    final Date value2AsDate = dateFormat.parse(value2AsString);
                    long value2 = value2AsDate.getTime();
                    value2 =
                        convertDayToTimezone(
                            ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), value2);
                    return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                  } catch (ParseException ignore) {
                    try {
                      final DateFormat dateFormat;
                      if (db != null) {
                        dateFormat = ODateHelper.getDateFormatInstance(db);
                      } else {
                        dateFormat =
                            new SimpleDateFormat(OStorageConfiguration.DEFAULT_DATETIME_FORMAT);
                      }

                      final Date value2AsDate = dateFormat.parse(value2AsString);
                      long value2 = value2AsDate.getTime();
                      value2 =
                          convertDayToTimezone(
                              ODateHelper.getDatabaseTimeZone(),
                              TimeZone.getTimeZone("GMT"),
                              value2);
                      return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                    } catch (ParseException ignored) {
                      return new Date(value1).toString().compareTo(value2AsString);
                    }
                  }
                }
              case DECIMAL:
                {
                  final long value2 =
                      ODecimalSerializer.INSTANCE
                          .deserialize(fieldValue2.bytes, fieldValue2.offset)
                          .longValue();
                  return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
                }
            }
            break;
          }

        case BINARY:
          {
            switch (iField2.type) {
              case BINARY:
                {
                  final int length1 = OVarIntSerializer.readAsInteger(fieldValue1);
                  final int length2 = OVarIntSerializer.readAsInteger(fieldValue2);

                  final int max = Math.min(length1, length2);
                  for (int i = 0; i < max; ++i) {
                    final byte b1 = fieldValue1.bytes[fieldValue1.offset + i];
                    final byte b2 = fieldValue2.bytes[fieldValue2.offset + i];

                    if (b1 > b2) return 1;
                    else if (b2 > b1) return -1;
                  }

                  if (length1 > length2) return 1;
                  else if (length2 > length1) return -1;

                  // EQUALS
                  return 0;
                }
            }
            break;
          }

        case LINK:
          {
            switch (iField2.type) {
              case LINK:
                {
                  final int clusterId1 = OVarIntSerializer.readAsInteger(fieldValue1);
                  final int clusterId2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  if (clusterId1 > clusterId2) return 1;
                  else if (clusterId1 < clusterId2) return -1;
                  else {
                    final long clusterPos1 = OVarIntSerializer.readAsLong(fieldValue1);
                    final long clusterPos2 = OVarIntSerializer.readAsLong(fieldValue2);
                    if (clusterPos1 > clusterPos2) return 1;
                    else if (clusterPos1 < clusterPos2) return -1;
                    return 0;
                  }
                }

              case STRING:
                {
                  return readOptimizedLink(fieldValue1, false)
                      .compareTo(new ORecordId(readString(fieldValue2)));
                }
            }
            break;
          }

        case DECIMAL:
          {
            final BigDecimal value1 =
                ODecimalSerializer.INSTANCE.deserialize(fieldValue1.bytes, fieldValue1.offset);

            switch (iField2.type) {
              case INTEGER:
                {
                  final int value2 = OVarIntSerializer.readAsInteger(fieldValue2);
                  return value1.compareTo(new BigDecimal(value2));
                }
              case LONG:
              case DATETIME:
                {
                  final long value2 = OVarIntSerializer.readAsLong(fieldValue2);
                  return value1.compareTo(new BigDecimal(value2));
                }
              case SHORT:
                {
                  final short value2 = OVarIntSerializer.readAsShort(fieldValue2);
                  return value1.compareTo(new BigDecimal(value2));
                }
              case FLOAT:
                {
                  final float value2 = Float.intBitsToFloat(readInteger(fieldValue2));
                  return value1.compareTo(new BigDecimal(value2));
                }
              case DOUBLE:
                {
                  final double value2 = Double.longBitsToDouble(readLong(fieldValue2));
                  return value1.compareTo(new BigDecimal(value2));
                }
              case STRING:
                {
                  final String value2 = readString(fieldValue2);
                  return value1.toString().compareTo(value2);
                }
              case DECIMAL:
                {
                  final BigDecimal value2 =
                      ODecimalSerializer.INSTANCE.deserialize(
                          fieldValue2.bytes, fieldValue2.offset);
                  return value1.compareTo(value2);
                }
              case BYTE:
                {
                  final byte value2 = readByte(fieldValue2);
                  return value1.compareTo(new BigDecimal(value2));
                }
            }
            break;
          }
      }
    } finally {
      fieldValue1.offset = offset1;
      fieldValue2.offset = offset2;
    }

    // NO COMPARE SUPPORTED, RETURN NON EQUALS
    return 1;
  }
}
