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

package com.orientechnologies.common.exception;

public abstract class OException extends RuntimeException {

  private static final long serialVersionUID = 3882447822497861424L;

  public static OException wrapException(final OException exception, final Throwable cause) {
    if (cause instanceof OHighLevelException)
      return (OException) cause;

    exception.initCause(cause);
    return exception;
  }

  public OException(final String message) {
    super(message);
  }

  /**
   * This constructor is needed to restore and reproduce exception on client side in case of remote storage exception handling.
   * Please create "copy constructor" for each exception which has current one as a parent.
   */
  public OException(final OException exception) {
    super(exception.getMessage(), exception.getCause());
  }

  /**
   * Passing of root exceptions directly is prohibited use {@link #wrapException(OException, Throwable)} instead.
   */
  private OException(final Throwable cause) {
    super(cause);
  }

  /**
   * Passing of root exceptions directly is prohibited use {@link #wrapException(OException, Throwable)} instead.
   */
  private OException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public static Throwable getFirstCause(Throwable iRootException) {
    while (iRootException.getCause() != null)
      iRootException = iRootException.getCause();

    return iRootException;
  }
}
