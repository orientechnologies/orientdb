/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

import com.orientechnologies.common.exception.OException;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 12/3/13
 */
public class ONestedRollbackException extends OException {
  public ONestedRollbackException(String message) {
    super(message);
  }

  public ONestedRollbackException(Throwable cause) {
    super(cause);
  }

  public ONestedRollbackException(String message, Throwable cause) {
    super(message, cause);
  }
}
