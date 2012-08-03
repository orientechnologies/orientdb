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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * Exception thrown when MVCC is enabled and a record cannot be updated or deleted because versions don't match.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OFastConcurrentModificationException extends OConcurrentModificationException {
  private static final long                                 serialVersionUID = 1L;
  private static final boolean                              ENABLED          = OGlobalConfiguration.DB_MVCC_THROWFAST
                                                                                 .getValueAsBoolean();
  private static final OFastConcurrentModificationException INSTANCE         = new OFastConcurrentModificationException();

  public OFastConcurrentModificationException() {
  }

  public static boolean enabled() {
    return ENABLED;
  }

  public static OFastConcurrentModificationException instance() {
    return INSTANCE;
  }
}
