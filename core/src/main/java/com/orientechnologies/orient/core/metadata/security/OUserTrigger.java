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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OSecurityManager;

/**
 * Encrypt the password using the SHA-256 algorithm.
 * 
 * @author Luca Garulli
 */
public class OUserTrigger extends ODocumentHookAbstract {
  private OClass userClass;
  private OClass roleClass;

  public OUserTrigger(ODatabaseDocument database) {
    super(database);

    setIncludeClasses(OUser.CLASS_NAME, ORole.CLASS_NAME);
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    init();

    if (iDocument.getSchemaClass().isSubClassOf(userClass))
      return encodePassword(iDocument);

    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    init();

    if (iDocument.getSchemaClass().isSubClassOf(userClass))
      return encodePassword(iDocument);

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

  private void init() {
    if (userClass == null)
      userClass = database.getMetadata().getSchema().getClass(OUser.CLASS_NAME);
    if (roleClass == null)
      roleClass = database.getMetadata().getSchema().getClass(ORole.CLASS_NAME);
  }
}
