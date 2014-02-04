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
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsBoolean;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsFloat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsInteger;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsList;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsLong;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsMap;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsSet;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodAsString;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodField;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFormat;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodFunctionDelegate;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodIndexOf;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodJavaType;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodKeys;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodNormalize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodPrefix;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodRemove;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodRemoveAll;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodSize;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToLowerCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodToUpperCase;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodTrim;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodType;
import com.orientechnologies.orient.core.sql.method.misc.OSQLMethodValues;

/**
 * Default methods factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultSQLMethodFactory implements OSQLMethodFactory {

  private final Map<String, Object> methods = new HashMap<String, Object>();

  public ODefaultSQLMethodFactory() {
    register(OSQLMethodAsBoolean.NAME, new OSQLMethodAsBoolean());
    register(OSQLMethodAsFloat.NAME, new OSQLMethodAsFloat());
    register(OSQLMethodAsInteger.NAME, new OSQLMethodAsInteger());
    register(OSQLMethodAsList.NAME, new OSQLMethodAsList());
    register(OSQLMethodAsLong.NAME, new OSQLMethodAsLong());
    register(OSQLMethodAsMap.NAME, new OSQLMethodAsMap());
    register(OSQLMethodAsSet.NAME, new OSQLMethodAsSet());
    register(OSQLMethodAsString.NAME, new OSQLMethodAsString());
    register(OSQLMethodField.NAME, new OSQLMethodField());
    register(OSQLMethodFormat.NAME, new OSQLMethodFormat());
    register(OSQLMethodFunctionDelegate.NAME, OSQLMethodFunctionDelegate.class);
    register(OSQLMethodIndexOf.NAME, new OSQLMethodIndexOf());
    register(OSQLMethodJavaType.NAME, new OSQLMethodJavaType());
    register(OSQLMethodKeys.NAME, new OSQLMethodKeys());
    register(OSQLMethodNormalize.NAME, new OSQLMethodNormalize());
    register(OSQLMethodPrefix.NAME, new OSQLMethodPrefix());
    register(OSQLMethodRemove.NAME, new OSQLMethodRemove());
    register(OSQLMethodRemoveAll.NAME, new OSQLMethodRemoveAll());
    register(OSQLMethodSize.NAME, new OSQLMethodSize());
    register(OSQLMethodToLowerCase.NAME, new OSQLMethodToLowerCase());
    register(OSQLMethodToUpperCase.NAME, new OSQLMethodToUpperCase());
    register(OSQLMethodTrim.NAME, new OSQLMethodTrim());
    register(OSQLMethodType.NAME, new OSQLMethodType());
    register(OSQLMethodValues.NAME, new OSQLMethodValues());
  }

  public void register(final String iName, final Object iImplementation) {
    methods.put(iName.toLowerCase(), iImplementation);
  }

  @Override
  public boolean hasMethod(final String iName) {
    return methods.containsKey(iName.toLowerCase());
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
