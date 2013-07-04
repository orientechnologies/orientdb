/*
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.sql.method;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAppend;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsBoolean;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDate;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDateTime;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsDecimal;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsFloat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsInteger;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsLong;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsString;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodCharAt;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFormat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFunctionDelegate;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodIndexOf;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodKeys;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodLeft;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodLength;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodNormalize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodPrefix;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodRemove;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodReplace;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodRight;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodSize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodSubString;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToJSON;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToLowerCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToUpperCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodTrim;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodValues;

/**
 * Default methods factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultSQLMethodFactory implements OSQLMethodFactory {

  private final Map<String, Object> methods = new HashMap<String, Object>();

  public ODefaultSQLMethodFactory() {
    methods.put(OSQLMethodAppend.NAME, new OSQLMethodAppend());
    methods.put(OSQLMethodAsBoolean.NAME, new OSQLMethodAsBoolean());
    methods.put(OSQLMethodAsDate.NAME, new OSQLMethodAsDate());
    methods.put(OSQLMethodAsDateTime.NAME, new OSQLMethodAsDateTime());
    methods.put(OSQLMethodAsDecimal.NAME, new OSQLMethodAsDecimal());
    methods.put(OSQLMethodAsFloat.NAME, new OSQLMethodAsFloat());
    methods.put(OSQLMethodAsInteger.NAME, new OSQLMethodAsInteger());
    methods.put(OSQLMethodAsLong.NAME, new OSQLMethodAsLong());
    methods.put(OSQLMethodAsString.NAME, new OSQLMethodAsString());
    methods.put(OSQLMethodCharAt.NAME, new OSQLMethodCharAt());
    methods.put(OSQLMethodField.NAME, new OSQLMethodField());
    methods.put(OSQLMethodFormat.NAME, new OSQLMethodFormat());
    methods.put(OSQLMethodFunctionDelegate.NAME, OSQLMethodFunctionDelegate.class);
    methods.put(OSQLMethodIndexOf.NAME, new OSQLMethodIndexOf());
    methods.put(OSQLMethodKeys.NAME, new OSQLMethodKeys());
    methods.put(OSQLMethodLeft.NAME, new OSQLMethodLeft());
    methods.put(OSQLMethodLength.NAME, new OSQLMethodLength());
    methods.put(OSQLMethodNormalize.NAME, new OSQLMethodNormalize());
    methods.put(OSQLMethodPrefix.NAME, new OSQLMethodPrefix());
    methods.put(OSQLMethodReplace.NAME, new OSQLMethodReplace());
    methods.put(OSQLMethodRemove.NAME, new OSQLMethodRemove());
    methods.put(OSQLMethodRight.NAME, new OSQLMethodRight());
    methods.put(OSQLMethodSize.NAME, new OSQLMethodSize());
    methods.put(OSQLMethodSubString.NAME, new OSQLMethodSubString());
    methods.put(OSQLMethodToJSON.NAME, new OSQLMethodToJSON());
    methods.put(OSQLMethodToLowerCase.NAME, new OSQLMethodToLowerCase());
    methods.put(OSQLMethodToUpperCase.NAME, new OSQLMethodToUpperCase());
    methods.put(OSQLMethodTrim.NAME, new OSQLMethodTrim());
    methods.put(OSQLMethodValues.NAME, new OSQLMethodValues());
  }

  @Override
  public boolean hasMethod(final String iName) {
    return methods.containsKey(iName);
  }

  @Override
  public Set<String> getMethodNames() {
    return methods.keySet();
  }

  @Override
  public OSQLMethod createMethod(final String name) throws OCommandExecutionException {
    final Object m = methods.get(name);
    final OSQLMethod method;

    if (m instanceof Class<?>)
      try {
        method = (OSQLMethod) ((Class<?>) m).newInstance();
      } catch (Exception e) {
        throw new OCommandExecutionException("Cannot create SQL method: " + m);
      }
    else
      method = (OSQLMethod) m;

    if (method == null)
      throw new OCommandExecutionException("Unknown method name: " + name);

    return method;
  }

}
