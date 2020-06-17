/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import java.util.Set;

/**
 * Checks the access against restricted resources. Restricted resources are those documents of
 * classes that implement ORestricted abstract class.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ORestrictedAccessHook {

  public static boolean onRecordBeforeCreate(
      final ODocument iDocument, ODatabaseDocumentInternal database) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {
      String fieldNames = cls.getCustom(OSecurityShared.ONCREATE_FIELD);
      if (fieldNames == null) fieldNames = ORestrictedOperation.ALLOW_ALL.getFieldName();
      final String[] fields = fieldNames.split(",");
      String identityType = cls.getCustom(OSecurityShared.ONCREATE_IDENTITY_TYPE);
      if (identityType == null) identityType = "user";

      OIdentifiable identity = null;
      if (identityType.equals("user")) {
        final OSecurityUser user = database.getUser();
        if (user != null) identity = user.getIdentity();
      } else if (identityType.equals("role")) {
        final Set<? extends OSecurityRole> roles = database.getUser().getRoles();
        if (!roles.isEmpty()) identity = roles.iterator().next().getIdentity();
      } else
        throw new OConfigurationException(
            "Wrong custom field '"
                + OSecurityShared.ONCREATE_IDENTITY_TYPE
                + "' in class '"
                + cls.getName()
                + "' with value '"
                + identityType
                + "'. Supported ones are: 'user', 'role'");

      if (identity != null) {
        for (String f : fields)
          database.getSharedContext().getSecurity().allowIdentity(database, iDocument, f, identity);
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean isAllowed(
      ODatabaseDocumentInternal database,
      final ODocument iDocument,
      final ORestrictedOperation iAllowOperation,
      final boolean iReadOriginal) {
    final OImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {

      if (database.getUser() == null) return true;

      if (database.getUser().isRuleDefined(ORule.ResourceGeneric.BYPASS_RESTRICTED, null))
        if (database
                .getUser()
                .checkIfAllowed(
                    ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ)
            != null)
          // BYPASS RECORD LEVEL SECURITY: ONLY "ADMIN" ROLE CAN BY DEFAULT
          return true;

      final ODocument doc;
      if (iReadOriginal)
        // RELOAD TO AVOID HACKING OF "_ALLOW" FIELDS
        doc = (ODocument) database.load(iDocument.getIdentity());
      else doc = iDocument;

      // we even not allowed to read it.
      if (doc == null) return false;

      return database
          .getMetadata()
          .getSecurity()
          .isAllowed(
              (Set<OIdentifiable>) doc.field(ORestrictedOperation.ALLOW_ALL.getFieldName()),
              (Set<OIdentifiable>) doc.field(iAllowOperation.getFieldName()));
    }

    return true;
  }
}
