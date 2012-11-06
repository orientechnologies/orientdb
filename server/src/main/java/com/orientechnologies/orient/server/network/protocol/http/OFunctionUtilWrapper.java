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
package com.orientechnologies.orient.server.network.protocol.http;

import com.orientechnologies.orient.core.metadata.function.OFunction;

/**
 * Wrapper of function with additional utility methods to help inside functions.
 * 
 * @author Luca Garulli
 * 
 */
public class OFunctionUtilWrapper {
  private OFunction f;

  public OFunctionUtilWrapper(final OFunction f) {
    this.f = f;
  }

  public boolean exists(final Object... iValues) {
    if (iValues != null)
      for (Object o : iValues)
        if (o != null && !o.equals("undefined"))
          return true;
    return false;
  }

  public Object value(final Object iValue) {
    return iValue != null ? iValue : null;
  }
}
