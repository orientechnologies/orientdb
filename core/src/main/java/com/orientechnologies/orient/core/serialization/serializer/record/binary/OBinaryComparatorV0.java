/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;

/**
 * Implementation v0 of comparator based on protocol v0.
 * 
 * @author Luca Garulli
 */
public class OBinaryComparatorV0 implements OBinaryComparator {

  public OBinaryComparatorV0() {
  }

  /**
   * Compares if 2 values are the same.
   *
   * @param iValue1
   *          First value to compare
   * @param iType1
   *          First value type
   * @param iValue2
   *          Second value to compare
   * @param iType2
   *          Second value type
   * @return true if they match, otherwise false
   */
  @Override
  public boolean isEqual(final BytesContainer iValue1, final OType iType1, final BytesContainer iValue2, final OType iType2) {
    final int offset1 = iValue1.offset;
    final int offset2 = iValue2.offset;

    try {
      switch (iType1) {
      case INTEGER: {
        final int value1 = OVarIntSerializer.readAsInteger(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case DATE: {
          final long value2 = (OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY);
          return value1 == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          return Integer.parseInt(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case LONG:
      case DATETIME: {
        final long value1 = OVarIntSerializer.readAsLong(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case DATE: {
          final long value2 = (OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY);
          return value1 == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          return Long.parseLong(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case SHORT: {
        final short value1 = OVarIntSerializer.readAsShort(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case DATE: {
          final long value2 = (OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY);
          return value1 == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          return Short.parseShort(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case STRING: {
        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return Integer.parseInt(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return Long.parseLong(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return Long.parseLong(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return Short.parseShort(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return Byte.parseByte(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return Float.parseFloat(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return Double.parseDouble(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        case STRING: {
          final int len1 = OVarIntSerializer.readAsInteger(iValue1);
          final int len2 = OVarIntSerializer.readAsInteger(iValue2);

          if (len1 != len2)
            return false;

          for (int i = 0; i < len1; ++i) {
            if (iValue1.bytes[offset1 + i] != iValue2.bytes[offset2 + i])
              return false;
          }
          return true;
        }
        case BOOLEAN: {
          final boolean value2 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;
          return Boolean.parseBoolean(ORecordSerializerBinaryV0.readString(iValue1)) == value2;
        }
        }
        break;
      }

      case DOUBLE: {
        final long value1AsLong = ORecordSerializerBinaryV0.readLong(iValue1);

        switch (iType2) {
        case INTEGER: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case SHORT: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2AsLong = ORecordSerializerBinaryV0.readLong(iValue2);
          return value1AsLong == value2AsLong;
        }
        case STRING: {
          final double value1 = Double.longBitsToDouble(value1AsLong);
          return Double.parseDouble(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case FLOAT: {
        final int value1AsInt = ORecordSerializerBinaryV0.readInteger(iValue1);

        switch (iType2) {
        case INTEGER: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case SHORT: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2AsInt = ORecordSerializerBinaryV0.readInteger(iValue2);
          return value1AsInt == value2AsInt;
        }
        case DOUBLE: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          final float value1 = Float.intBitsToFloat(value1AsInt);
          return Float.parseFloat(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case BYTE: {
        final byte value1 = ORecordSerializerBinaryV0.readByte(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case BYTE: {
          final byte value2 = ORecordSerializerBinaryV0.readByte(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          final byte value2 = Byte.parseByte((ORecordSerializerBinaryV0.readString(iValue2)));
          return value1 == value2;
        }
        }
        break;
      }

      case BOOLEAN: {
        final boolean value1 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;

        switch (iType2) {
        case BOOLEAN: {
          final boolean value2 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;
          return value1 == value2;
        }
        case STRING: {
          final String str = ORecordSerializerBinaryV0.readString(iValue2);
          return Boolean.parseBoolean(str) == value1;
        }
        }
        break;
      }

      case DATE: {
        final long value1 = OVarIntSerializer.readAsLong(iValue1) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1 == value2;
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1 == value2;
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return value1 == value2;
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1 == value2;
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1 == value2;
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1 == value2;
        }
        case STRING: {
          return Short.parseShort(ORecordSerializerBinaryV0.readString(iValue2)) == value1;
        }
        }
        break;
      }

      case BINARY: {
        switch (iType2) {
        case BINARY: {
          final int length1 = OVarIntSerializer.readAsInteger(iValue1);
          final int length2 = OVarIntSerializer.readAsInteger(iValue2);
          if (length1 != length2)
            return false;

          for (int i = 0; i < length1; ++i) {
            if (iValue1.bytes[offset1 + i] != iValue2.bytes[offset2 + i])
              return false;
          }
          return true;
        }
        }
        break;
      }

      case LINK: {
        switch (iType2) {
        case LINK: {
          final int clusterId1 = OVarIntSerializer.readAsInteger(iValue1);
          final int clusterId2 = OVarIntSerializer.readAsInteger(iValue2);
          if (clusterId1 != clusterId2)
            return false;

          final long clusterPos1 = OVarIntSerializer.readAsLong(iValue1);
          final long clusterPos2 = OVarIntSerializer.readAsLong(iValue2);
          if (clusterPos1 == clusterPos2)
            return true;
        }
        case STRING: {
          return ORecordSerializerBinaryV0.readOptimizedLink(iValue1).toString()
              .equals(ORecordSerializerBinaryV0.readString(iValue2));
        }
        }
        break;
      }

      case DECIMAL: {
        final BigDecimal value1 = ODecimalSerializer.INSTANCE.deserialize(iValue1.bytes, iValue1.offset);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1.equals(new BigDecimal(value2));
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1.equals(new BigDecimal(value2));
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1.equals(new BigDecimal(value2));
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1.equals(new BigDecimal(value2));
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1.equals(new BigDecimal(value2));
        }
        case STRING: {
          return value1.toString().equals(ORecordSerializerBinaryV0.readString(iValue2));
        }
        }
        break;
      }
      }
    } finally {
      iValue1.offset = offset1;
      iValue2.offset = offset2;
    }

    return false;
  }

  /**
   * Compares two values executing also conversion between types.
   *
   * @param iValue1
   *          First value to compare
   * @param iType1
   *          First value type
   * @param iValue2
   *          Second value to compare
   * @param iType2
   *          Second value type
   * @return 0 if they matches, >0 if first value is major than second, <0 in case is minor
   */
  @Override
  public int compare(final BytesContainer iValue1, final OType iType1, final BytesContainer iValue2, final OType iType2) {
    final int offset1 = iValue1.offset;
    final int offset2 = iValue2.offset;

    try {
      switch (iType1) {
      case INTEGER: {
        final int value1 = OVarIntSerializer.readAsInteger(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final int value2 = Integer.parseInt(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case LONG: {
        final long value1 = OVarIntSerializer.readAsLong(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final long value2 = Long.parseLong(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case SHORT: {
        final short value1 = OVarIntSerializer.readAsShort(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final short value2 = Short.parseShort(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case STRING: {
        final String value1 = ORecordSerializerBinaryV0.readString(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1.compareTo(Integer.toString(value2));
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1.compareTo(Long.toString(value2));
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return value1.compareTo(Long.toString(value2));
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1.compareTo(Short.toString(value2));
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return value1.compareTo(Byte.toString(value2));
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1.compareTo(Float.toString(value2));
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1.compareTo(Double.toString(value2));
        }
        case STRING: {
          final String value2 = ORecordSerializerBinaryV0.readString(iValue1);
          return value1.compareTo(value2);
        }
        case BOOLEAN: {
          final boolean value2 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;
          return value1.compareTo(Boolean.toString(value2));
        }
        }
        break;
      }

      case DOUBLE: {
        final double value1 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue1));

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final double value2 = Double.parseDouble(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case FLOAT: {
        final float value1 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue1));

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final float value2 = Float.parseFloat(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case BYTE: {
        final byte value1 = ORecordSerializerBinaryV0.readByte(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = ORecordSerializerBinaryV0.readByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final byte value2 = Byte.parseByte((ORecordSerializerBinaryV0.readString(iValue2)));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        }
        break;
      }

      case BOOLEAN: {
        final boolean value1 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;

        switch (iType2) {
        case BOOLEAN: {
          final boolean value2 = ORecordSerializerBinaryV0.readByte(iValue1) == 1;
          return (value1 == value2) ? 0 : value1 ? 1 : 0;
        }
        case STRING: {
          final boolean value2 = Boolean.parseBoolean(ORecordSerializerBinaryV0.readString(iValue2));
          return (value1 == value2) ? 0 : value1 ? 1 : 0;
        }
        }
        break;
      }

      case DATETIME: {
        final long value1 = OVarIntSerializer.readAsLong(iValue1);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case BYTE: {
          final byte value2 = OVarIntSerializer.readAsByte(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final String value2AsString = ORecordSerializerBinaryV0.readString(iValue2);

          if (OIOUtils.isLong(value2AsString)) {
            final long value2 = Long.parseLong(value2AsString);
            return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
          }

          try {
            final Date value2AsDate = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration()
                .getDateFormatInstance().parse(value2AsString);
            final long value2 = value2AsDate.getTime();
            return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
          } catch (ParseException e) {
            try {
              final Date value2AsDate = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration()
                  .getDateTimeFormatInstance().parse(value2AsString);
              final long value2 = value2AsDate.getTime();
              return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
            } catch (ParseException e1) {
              return new Date(value1).toString().compareTo(value2AsString);
            }
          }
        }
        }
        break;
      }
      case DATE: {
        final long value1 = OVarIntSerializer.readAsLong(iValue1) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case LONG:
        case DATETIME: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DATE: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2) * ORecordSerializerBinaryV0.MILLISEC_PER_DAY;
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
        }
        case STRING: {
          final String value2AsString = ORecordSerializerBinaryV0.readString(iValue2);

          if (OIOUtils.isLong(value2AsString)) {
            final long value2 = Long.parseLong(value2AsString);
            return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
          }

          try {
            final Date value2AsDate = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration()
                .getDateTimeFormatInstance().parse(value2AsString);
            final long value2 = value2AsDate.getTime();
            return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
          } catch (ParseException e) {
            try {
              final Date value2AsDate = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration()
                  .getDateFormatInstance().parse(value2AsString);
              final long value2 = value2AsDate.getTime();
              return (value1 < value2) ? -1 : ((value1 == value2) ? 0 : 1);
            } catch (ParseException e1) {
              return new Date(value1).toString().compareTo(value2AsString);
            }
          }
        }
        }
        break;
      }

      case BINARY: {
        switch (iType2) {
        case BINARY: {
          final int length1 = OVarIntSerializer.readAsInteger(iValue1);
          final int length2 = OVarIntSerializer.readAsInteger(iValue2);

          for (int i = 0; i < length1; ++i) {
            final byte b1 = iValue1.bytes[offset1 + i];
            final byte b2 = iValue2.bytes[offset2 + i];

            if (b1 > b2)
              return 1;
            else if (b2 > b1)
              return -1;
          }

          // EQUALS
          return 0;
        }
        }
        break;
      }

      case LINK: {
        switch (iType2) {
        case LINK: {
          final int clusterId1 = OVarIntSerializer.readAsInteger(iValue1);
          final int clusterId2 = OVarIntSerializer.readAsInteger(iValue2);
          if (clusterId1 > clusterId2)
            return 1;
          else if (clusterId1 < clusterId2)
            return -1;
          else {
            final long clusterPos1 = OVarIntSerializer.readAsLong(iValue1);
            final long clusterPos2 = OVarIntSerializer.readAsLong(iValue2);
            if (clusterPos1 > clusterPos2)
              return 1;
            else if (clusterPos1 < clusterPos2)
              return -1;
            return 0;
          }
        }

        case STRING: {
          return ORecordSerializerBinaryV0.readOptimizedLink(iValue1).compareTo(
              new ORecordId(ORecordSerializerBinaryV0.readString(iValue2)));
        }
        }
        break;
      }

      case DECIMAL: {
        final BigDecimal value1 = ODecimalSerializer.INSTANCE.deserialize(iValue1.bytes, iValue1.offset);

        switch (iType2) {
        case INTEGER: {
          final int value2 = OVarIntSerializer.readAsInteger(iValue2);
          return value1.compareTo(new BigDecimal(value2));
        }
        case LONG: {
          final long value2 = OVarIntSerializer.readAsLong(iValue2);
          return value1.compareTo(new BigDecimal(value2));
        }
        case SHORT: {
          final short value2 = OVarIntSerializer.readAsShort(iValue2);
          return value1.compareTo(new BigDecimal(value2));
        }
        case FLOAT: {
          final float value2 = Float.intBitsToFloat(ORecordSerializerBinaryV0.readInteger(iValue2));
          return value1.compareTo(new BigDecimal(value2));
        }
        case DOUBLE: {
          final double value2 = Double.longBitsToDouble(ORecordSerializerBinaryV0.readLong(iValue2));
          return value1.compareTo(new BigDecimal(value2));
        }
        case STRING: {
          final String value2 = ORecordSerializerBinaryV0.readString(iValue2);
          return value1.compareTo(new BigDecimal(value2));
        }
        }
        break;
      }
      }
    } finally {
      iValue1.offset = offset1;
      iValue2.offset = offset2;
    }

    // NO COMPARE SUPPORTED, RETURN NON EQUALS
    return 1;
  }
}
