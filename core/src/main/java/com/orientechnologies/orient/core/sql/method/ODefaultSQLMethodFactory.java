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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.coll.OSQLMethodMultiValue;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLMethodAsDate;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLMethodAsDateTime;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLMethodAsDecimal;
import com.orientechnologies.orient.core.sql.functions.conversion.OSQLMethodConvert;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLMethodExclude;
import com.orientechnologies.orient.core.sql.functions.misc.OSQLMethodInclude;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodAppend;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodFromJSON;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodHash;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodLength;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodReplace;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodRight;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodSubString;
import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodToJSON;
import com.orientechnologies.orient.core.sql.method.misc.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default method factory.
 * 
 * @author Johann Sorel (Geomatys)
 */
public class ODefaultSQLMethodFactory implements OSQLMethodFactory {

  private final Map<String, Object> methods = new HashMap<String, Object>();

  public ODefaultSQLMethodFactory() {
    register(OSQLMethodAppend.NAME, new OSQLMethodAppend());
    register(OSQLMethodAsBoolean.NAME, new OSQLMethodAsBoolean());
    register(OSQLMethodAsDate.NAME, new OSQLMethodAsDate());
    register(OSQLMethodAsDateTime.NAME, new OSQLMethodAsDateTime());
    register(OSQLMethodAsDecimal.NAME, new OSQLMethodAsDecimal());
    register(OSQLMethodAsFloat.NAME, new OSQLMethodAsFloat());
    register(OSQLMethodAsInteger.NAME, new OSQLMethodAsInteger());
    register(OSQLMethodAsList.NAME, new OSQLMethodAsList());
    register(OSQLMethodAsLong.NAME, new OSQLMethodAsLong());
    register(OSQLMethodAsMap.NAME, new OSQLMethodAsMap());
    register(OSQLMethodAsSet.NAME, new OSQLMethodAsSet());
    register(OSQLMethodAsString.NAME, new OSQLMethodAsString());
    register(OSQLMethodCharAt.NAME, new OSQLMethodCharAt());
    register(OSQLMethodConvert.NAME, new OSQLMethodConvert());
    register(OSQLMethodExclude.NAME, new OSQLMethodExclude());
    register(OSQLMethodField.NAME, new OSQLMethodField());
    register(OSQLMethodFormat.NAME, new OSQLMethodFormat());
    register(OSQLMethodFromJSON.NAME, new OSQLMethodFromJSON());
    register(OSQLMethodFunctionDelegate.NAME, OSQLMethodFunctionDelegate.class);
    register(OSQLMethodHash.NAME, new OSQLMethodHash());
    register(OSQLMethodInclude.NAME, new OSQLMethodInclude());
    register(OSQLMethodIndexOf.NAME, new OSQLMethodIndexOf());
    register(OSQLMethodJavaType.NAME, new OSQLMethodJavaType());
    register(OSQLMethodKeys.NAME, new OSQLMethodKeys());
    register(OSQLMethodLastIndexOf.NAME, new OSQLMethodLastIndexOf());
    register(OSQLMethodLeft.NAME, new OSQLMethodLeft());
    register(OSQLMethodLength.NAME, new OSQLMethodLength());
    register(OSQLMethodMultiValue.NAME, new OSQLMethodMultiValue());
    register(OSQLMethodNormalize.NAME, new OSQLMethodNormalize());
    register(OSQLMethodPrefix.NAME, new OSQLMethodPrefix());
    register(OSQLMethodRemove.NAME, new OSQLMethodRemove());
    register(OSQLMethodRemoveAll.NAME, new OSQLMethodRemoveAll());
    register(OSQLMethodReplace.NAME, new OSQLMethodReplace());
    register(OSQLMethodRight.NAME, new OSQLMethodRight());
    register(OSQLMethodSize.NAME, new OSQLMethodSize());
    register(OSQLMethodToLowerCase.NAME, new OSQLMethodToLowerCase());
    register(OSQLMethodToUpperCase.NAME, new OSQLMethodToUpperCase());
    register(OSQLMethodTrim.NAME, new OSQLMethodTrim());
    register(OSQLMethodType.NAME, new OSQLMethodType());
    register(OSQLMethodSubString.NAME, new OSQLMethodSubString());
    register(OSQLMethodToJSON.NAME, new OSQLMethodToJSON());
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
        throw new OCommandExecutionException("Cannot create SQL method: " + m, e);
      }
    else
      method = (OSQLMethod) m;

    if (method == null)
      throw new OCommandExecutionException("Unknown method name: " + name);

    return method;
  }

}
