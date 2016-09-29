/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 */
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.OHighLevelException;

/**
 * Indicates that the requested record content was not found in the database. Typically, this happens when the record was deleted.
 */
public class ORecordContentNotFoundException extends OCoreException implements OHighLevelException {
  private static final long serialVersionUID = 1;

  private final Object context;

  /**
   * Constructs a new instance of this exception class from another instance of it. Implicitly used by the network deserialization.
   *
   * @param exception the exception instance to construct the new one from.
   */
  @SuppressWarnings("unused")
  public ORecordContentNotFoundException(ORecordContentNotFoundException exception) {
    super(exception);
    this.context = exception.context;
  }

  /**
   * Constructs a new instance of this exception class for the provided context object.
   *
   * @param context the context object. Since the actual record maybe not known at this point, but there is still a need to provide
   *                some context to distinguish this exception from the others.
   */
  public ORecordContentNotFoundException(Object context) {
    super("Unable to find record content. The record or its content was deleted. Context: " + context);
    this.context = context;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ORecordContentNotFoundException))
      return false;
    final ORecordContentNotFoundException other = (ORecordContentNotFoundException) obj;

    return context == other.context || context != null && context.equals(other.context);
  }

}
