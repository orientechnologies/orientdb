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
package com.orientechnologies.common.concur;

import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.orient.core.exception.OCoreException;

/**
 * Abstract base exception to extend for all the exception that report to the user it has been
 * thrown but re-executing it could succeed.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class ONeedRetryException extends OCoreException {
  private static final long serialVersionUID = 1L;

  protected ONeedRetryException(final ONeedRetryException exception) {
    super(exception, null);
  }

  protected ONeedRetryException(final ONeedRetryException exception, OErrorCode errorCode) {
    super(exception, errorCode);
  }

  protected ONeedRetryException(final String message, OErrorCode errorCode) {
    super(message, null, errorCode);
  }

  protected ONeedRetryException(final String message) {
    super(message);
  }
}
