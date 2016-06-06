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
package com.orientechnologies.orient.stresstest.util;

import com.orientechnologies.orient.stresstest.OMode;

/**
 * Some error messages defined as constants
 *
 * @author Andrea Iacono
 */
public class OErrorMessages {

    public static final String OPERATION_SET_SHOULD_CONTAIN_ALL_MESSAGE = "OOperationsSet value should contain all of 'c', 'r', 'u' and 'd' characters.";
    public static final String OPERATION_SET_INVALID_FORM_MESSAGE = "OperationSet must be in form of CxIxUxDx where x is a valid number.";

    public static final String COMMAND_LINE_PARSER_INVALID_NUMBER = "Invalid %s number [%s].";
    public static final String COMMAND_LINE_PARSER_LESSER_THAN_ZERO_NUMBER = "The %s value must be greater than 0.";
    public static final String COMMAND_LINE_PARSER_INVALID_MODE = "Invalid mode [%s].";
    public static final String COMMAND_LINE_PARSER_INVALID_OPTION = "Invalid option [%s]";
    public static final String COMMAND_LINE_PARSER_EXPECTED_VALUE = "Expected value after argument [%s]";
    public static final String COMMAND_LINE_PARSER_DELETES_GT_CREATES = "The number of deletes [%d] is greater than the number of creates [%d].";
    public static final String COMMAND_LINE_PARSER_READS_GT_CREATES = "The number of reads [%d] is greater than the number of creates [%d].";
    public static final String COMMAND_LINE_PARSER_INVALID_REMOTE_PORT_NUMBER = "Invalid remote port [%d]. The port number has to be lesser than 65536.";
    public static final String COMMAND_LINE_PARSER_MISSING_REMOTE_IP = "The mode is [" + OMode.REMOTE + "] but the param --" + OConstants.OPTION_REMOTE_IP + " wasn't passed.";
    public static final String ERROR_OPENING_CONSOLE = "An error has occurred opening the console. Please supply the root password as the -" + OConstants.OPTION_ROOT_PASSWORD + " parameter.";
    public static final String COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY = "The mode param [-m] is mandatory.";
}
