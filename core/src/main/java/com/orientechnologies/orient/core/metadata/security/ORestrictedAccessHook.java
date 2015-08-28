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
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;

import java.util.Set;

/**
 * Checks the access against restricted resources. Restricted resources are those documents of classes that implement ORestricted
 * abstract class.
 * 
 * @author Luca Garulli
 */
public class ORestrictedAccessHook extends ODocumentHookAbstract {

  public ORestrictedAccessHook(ODatabaseDocument database) {
    super(database);
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (cls != null && cls.isRestricted()) {
      String fieldNames = cls.getCustom(OSecurityShared.ONCREATE_FIELD);
      if (fieldNames == null)
        fieldNames = ORestrictedOperation.ALLOW_ALL.getFieldName();
      final String[] fields = fieldNames.split(",");
      String identityType = cls.getCustom(OSecurityShared.ONCREATE_IDENTITY_TYPE);
      if (identityType == null)
        identityType = "user";

      OIdentifiable identity = null;
      if (identityType.equals("user")) {
        final OSecurityUser user = database.getUser();
        if (user != null)
          identity = user.getIdentity();
      } else if (identityType.equals("role")) {
        final Set<? extends OSecurityRole> roles = database.getUser().getRoles();
        if (!roles.isEmpty())
          identity = roles.iterator().next().getIdentity();
      } else
        throw new OConfigurationException("Wrong custom field '" + OSecurityShared.ONCREATE_IDENTITY_TYPE + "' in class '"
            + cls.getName() + "' with value '" + identityType + "'. Supported ones are: 'user', 'role'");

      if (identity != null) {
        for (String f : fields)
          database.getMetadata().getSecurity().allowIdentity(iDocument, f, identity);
        return RESULT.RECORD_CHANGED;
      }
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeRead(final ODocument iDocument) {
    return isAllowed(iDocument, ORestrictedOperation.ALLOW_READ, false) ? RESULT.RECORD_NOT_CHANGED : RESULT.SKIP;
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    if (!isAllowed(iDocument, ORestrictedOperation.ALLOW_UPDATE, true))
      throw new OSecurityException("Cannot update record " + iDocument.getIdentity() + ": the resource has restricted access");
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    if (!isAllowed(iDocument, ORestrictedOperation.ALLOW_DELETE, true))
      throw new OSecurityException("Cannot delete record " + iDocument.getIdentity() + ": the resource has restricted access");
    return RESULT.RECORD_NOT_CHANGED;
  }

  @SuppressWarnings("unchecked")
  protected boolean isAllowed(final ODocument iDocument, final ORestrictedOperation iAllowOperation, final boolean iReadOriginal) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (cls != null && cls.isRestricted()) {

      if (database.getUser() == null)
        return true;

      if (database.getUser().isRuleDefined(ORule.ResourceGeneric.BYPASS_RESTRICTED, null))
        if (database.getUser().checkIfAllowed(ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ) != null)
          // BYPASS RECORD LEVEL SECURITY: ONLY "ADMIN" ROLE CAN BY DEFAULT
          return true;

      final ODocument doc;
      if (iReadOriginal)
        // RELOAD TO AVOID HACKING OF "_ALLOW" FIELDS
        doc = (ODocument) database.load(iDocument.getIdentity());
      else
        doc = iDocument;

      // we even not allowed to read it.
      if (doc == null)
        return false;

      return database
          .getMetadata()
          .getSecurity()
          .isAllowed((Set<OIdentifiable>) doc.field(ORestrictedOperation.ALLOW_ALL.getFieldName()),
              (Set<OIdentifiable>) doc.field(iAllowOperation.getFieldName()));
    }

    return true;
  }
}
