/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.distributed.ODistributedException;

/**
 * Hot Aligment is not possible, if autoDeploy:true restore of entire database
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHotAlignmentNotPossibleException extends ODistributedException {
  public OHotAlignmentNotPossibleException(OHotAlignmentNotPossibleException exception) {
    super(exception);
  }

  public OHotAlignmentNotPossibleException(String s) {
    super(s);
  }
}
