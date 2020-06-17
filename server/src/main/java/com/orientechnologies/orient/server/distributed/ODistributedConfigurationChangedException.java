/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
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

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OHighLevelException;

/**
 * Cluster id was reassigned during the operation. The client should catch this exception and retry
 * to execute the same operation.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class ODistributedConfigurationChangedException extends ONeedRetryException
    implements OHighLevelException {
  public ODistributedConfigurationChangedException(
      ODistributedConfigurationChangedException exception) {
    super(exception);
  }

  public ODistributedConfigurationChangedException(String s) {
    super(s);
  }
}
