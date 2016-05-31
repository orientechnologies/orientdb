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

/**
 * This class represents the operation set parameter. Its format is specified in docs.
 *
 * @author Andrea Iacono
 */
public class OOperationsSet {

    private int reads;
    private int creates;
    private int updates;
    private int deletes;

    /**
     * creates an OperationSet
     *
     * @param ops
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
                    creates = valPos.value;
                    break;
                case 'R':
                    reads = valPos.value;
                    if (reads > creates) {
                        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_READS_GT_CREATES, reads, creates));
                    }
                    break;
                case 'U':
                    updates = valPos.value;
                    break;
                case 'D':
                    deletes = valPos.value;
                    if (deletes > creates) {
                        throw new OInitException(String.format(OErrorMessages.COMMAND_LINE_PARSER_DELETES_GT_CREATES, deletes, creates));
                    }
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

    @Override
    public String toString() {
        return String.format("[Creates: %d - Reads: %d - Updates: %d - Deletes: %d]", creates, reads, updates, deletes);
    }

    public int getNumberOfCreates() {
        return creates;
    }

    public int getNumberOfReads() {
        return reads;
    }

    public int getNumberOfUpdates() {
        return updates;
    }

    public int getNumberOfDeletes() {
        return deletes;
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
