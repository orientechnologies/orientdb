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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;

/**
 * Encrypt the password using the SHA-256 algorithm.
 * 
 * @author Luca Garulli
 */
public class OUserTrigger extends ODocumentHookAbstract {
  public OUserTrigger() {
    setIncludeClasses("OUser", "ORole");
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    if ("OUser".equalsIgnoreCase(iDocument.getClassName()))
      return encodePassword(iDocument);
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {

    if ("OUser".equalsIgnoreCase(iDocument.getClassName())) {
      // REMOVE THE USER FROM THE CACHE
      final OSecurity sec = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getUnderlying();
      if (sec instanceof OSecurityShared)
        ((OSecurityShared) sec).uncacheUser((String) iDocument.field("name"));

      return encodePassword(iDocument);

    } else if ("ORole".equalsIgnoreCase(iDocument.getClassName())) {
      final OSecurity sec = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSecurity().getUnderlying();
      if (sec instanceof OSecurityShared)
        ((OSecurityShared) sec).uncacheRole((String) iDocument.field("name"));
    }

    return RESULT.RECORD_NOT_CHANGED;
  }

  private RESULT encodePassword(final ODocument iDocument) {
    if (iDocument.field("name") == null)
      throw new OSecurityException("User name not found");

    final String password = (String) iDocument.field("password");

    if (password == null)
      throw new OSecurityException("User '" + iDocument.field("name") + "' has no password");

    if (!password.startsWith(OSecurityManager.ALGORITHM_PREFIX)) {
      iDocument.field("password", OUser.encryptPassword(password));
      return RESULT.RECORD_CHANGED;
    }

    return RESULT.RECORD_NOT_CHANGED;
  }
}
