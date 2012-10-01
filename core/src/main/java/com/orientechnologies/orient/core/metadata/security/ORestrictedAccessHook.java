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

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Checks the access against restricted resources. Restricted resources are those documents of classes that implement ORestricted
 * abstract class.
 * 
 * @author Luca Garulli
 */
public class ORestrictedAccessHook extends ODocumentHookAbstract {
  public ORestrictedAccessHook() {
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    final OClass cls = iDocument.getSchemaClass();
    if (cls != null && cls.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {
      Set<OIdentifiable> allowed = iDocument.field(OSecurityShared.ALLOW_FIELD);
      if (allowed == null) {
        allowed = new HashSet<OIdentifiable>();
        iDocument.field(OSecurityShared.ALLOW_FIELD, allowed);
      }
      allowed.add(ODatabaseRecordThreadLocal.INSTANCE.get().getUser().getDocument().getIdentity());

      return RESULT.RECORD_CHANGED;
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeRead(final ODocument iDocument) {
    return isAllowed(iDocument, false) ? RESULT.RECORD_NOT_CHANGED : RESULT.SKIP;
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    if (!isAllowed(iDocument, true))
      throw new OSecurityException("Resource " + iDocument.getIdentity() + " has restricted access");
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    if (!isAllowed(iDocument, true))
      throw new OSecurityException("Resource " + iDocument.getIdentity() + " has restricted access");
    return RESULT.RECORD_NOT_CHANGED;
  }

  @SuppressWarnings("unchecked")
  protected boolean isAllowed(final ODocument iDocument, final boolean iReadOriginal) {
    final OClass cls = iDocument.getSchemaClass();
    if (cls != null && cls.isSubClassOf(OSecurityShared.RESTRICTED_CLASSNAME)) {

      final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

      if (db.getUser().isRuleDefined(ODatabaseSecurityResources.BYPASS_RESTRICTED))
        // BYPASS RECORD LEVEL SECURITY: ONLY "ADMIN" ROLE CAN BY DEFAULT
        return true;

      final ODocument doc;
      if (iReadOriginal)
        // RELOAD TO AVOID HACKING OF "_ALLOW" FIELD
        doc = (ODocument) db.load(iDocument.getIdentity());
      else
        doc = iDocument;

      return db.getMetadata().getSecurity().isAllowed((Set<OIdentifiable>) doc.field(OSecurityShared.ALLOW_FIELD));
    }

    return true;
  }
}
