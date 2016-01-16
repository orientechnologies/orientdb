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

package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin
 * @since 8/30/13
 */
public class OSBTreeException extends OException {
  public OSBTreeException() {
  }

  public OSBTreeException(String message) {
    super(message);
  }

  public OSBTreeException(Throwable cause) {
    super(cause);
  }

  public OSBTreeException(String message, Throwable cause) {
    super(message, cause);
  }
}
