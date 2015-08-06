/*
 * Copyright 2013 Orient Technologies.
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
package com.orientechnologies.orient.core.sql.functions.text;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.security.OSecurityManager;
import com.orientechnologies.orient.core.sql.method.misc.OAbstractSQLMethod;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * Hash a string supporting multiple algorithm, all those supported by JVM
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSQLMethodHash extends OAbstractSQLMethod {

  public static final String NAME = "hash";

  public OSQLMethodHash() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "hash([<algorithm>])";
  }

  @Override
  public Object execute(final Object iThis, final OIdentifiable iCurrentRecord, final OCommandContext iContext,
      final Object ioResult, final Object[] iParams) {
    if (iThis == null)
      return null;

    final String algorithm = iParams.length > 0 ? iParams[0].toString() : OSecurityManager.ALGORITHM;
    try {
      return OSecurityManager.digest2String(iThis.toString(), algorithm);

    } catch (NoSuchAlgorithmException e) {
      throw new OCommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported", e);
    } catch (UnsupportedEncodingException e) {
      throw new OCommandExecutionException("hash(): encoding 'UTF-8' is not supported", e);
    }
  }
}
