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

public class OException extends RuntimeException {

  private static final long serialVersionUID = 3882447822497861424L;

  public OException() {
  }

  public OException(final String message) {
    super(message);
  }

  public OException(final Throwable cause) {
    super(cause);
  }

  public OException(final String message, final Throwable cause) {
    super(message, cause);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass()))
      return false;

    final String myMsg = getMessage();
    final String otherMsg = ((OException) obj).getMessage();
    if (myMsg == null || otherMsg == null)
      // UNKNOWN
      return false;

    return myMsg.equals(otherMsg);
  }

  public static Throwable getFirstCause(Throwable iRootException) {
    while (iRootException.getCause() != null)
      iRootException = iRootException.getCause();

    return iRootException;
  }
}
