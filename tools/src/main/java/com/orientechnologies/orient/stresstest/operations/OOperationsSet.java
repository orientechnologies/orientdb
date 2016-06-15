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
package com.orientechnologies.orient.stresstest.operations;

import com.orientechnologies.orient.stresstest.util.OErrorMessages;
import com.orientechnologies.orient.stresstest.util.OInitException;

import java.util.HashMap;
import java.util.Map;

/**
 * This class represents the operation set parameter. Its format is specified in docs.
 *
 * @author Andrea Iacono
 */
public class OOperationsSet {

  private long total;

  // this field needs to be public in order to have JSON Mapper to show it in results
  public Map<OOperationType, Integer> number = new HashMap<OOperationType, Integer>();

  /**
   * creates an OperationSet
   *
   * @param ops the string containing the operation to execute
   * @throws Exception if the param format is not correct
   */
  public OOperationsSet(String ops) throws OInitException {

    ops = ops.toUpperCase();
    if (!(ops.contains("C") && ops.contains("R") && ops.contains("U") && ops.contains("D"))) {
      throw new OInitException(OErrorMessages.OPERATION_SET_SHOULD_CONTAIN_ALL_MESSAGE);
    }

    int pos = 0;
    while (pos < ops.length()) {

      ValuePosition valPos = getValue(ops, pos + 1);
      switch (ops.charAt(pos)) {
      case 'C':
        number.put(OOperationType.CREATE, valPos.value);
        total += valPos.value;
        break;
      case 'R':
        number.put(OOperationType.READ, valPos.value);
        if (number.get(OOperationType.READ) > number.get(OOperationType.CREATE)) {
          throw new OInitException(String
              .format(OErrorMessages.COMMAND_LINE_PARSER_READS_GT_CREATES, number.get(OOperationType.READ),
                  number.get(OOperationType.CREATE)));
        }
        total += valPos.value;
        break;
      case 'U':
        number.put(OOperationType.UPDATE, valPos.value);
        total += valPos.value;
        break;
      case 'D':
        number.put(OOperationType.DELETE, valPos.value);
        if (number.get(OOperationType.DELETE) > number.get(OOperationType.CREATE)) {
          throw new OInitException(String
              .format(OErrorMessages.COMMAND_LINE_PARSER_DELETES_GT_CREATES, number.get(OOperationType.DELETE),
                  number.get(OOperationType.CREATE)));
        }
        total += valPos.value;
        break;
      default:
        throw new OInitException(OErrorMessages.OPERATION_SET_INVALID_FORM_MESSAGE);
      }
      pos += valPos.position + 1;
    }
  }

  private ValuePosition getValue(String ops, int pos) throws OInitException {

    int valueLength = 0;
    while (pos + valueLength < ops.length()) {
      if (ops.charAt(pos + valueLength) < '0' || ops.charAt(pos + valueLength) > '9') {
        break;
      }
      valueLength++;
    }

    String value = ops.substring(pos, pos + valueLength);
    try {
      return new ValuePosition(Integer.parseInt(value), valueLength);
    } catch (NumberFormatException nfe) {
      throw new OInitException(OErrorMessages.OPERATION_SET_INVALID_FORM_MESSAGE);
    }
  }

  @Override public String toString() {
    return String.format(
        "[Creates: %,d - Reads: %,d - Updates: %,d - Deletes: %,d]",
        number.get(OOperationType.CREATE),
        number.get(OOperationType.READ),
        number.get(OOperationType.UPDATE),
        number.get(OOperationType.DELETE)
    );
  }

  public long getNumber(OOperationType type) {
    return number.get(type);
  }

  public long getTotalOperations() {
    return total;
  }

  /**
   * when Java will support tuples I'll be an happy guy
   **/
  private class ValuePosition {
    int value;
    int position;

    public ValuePosition(int value, int position) {
      this.value = value;
      this.position = position;
    }
  }
}
