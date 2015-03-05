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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;
import java.util.Set;

/**
 * Manages users and roles.
 * 
 * @author Luca Garulli
 * 
 */
public interface OSecurity {
  public static final String RESTRICTED_CLASSNAME   = "ORestricted";
  public static final String IDENTITY_CLASSNAME     = "OIdentity";
  public static final String ALLOW_ALL_FIELD        = "_allow";
  public static final String ALLOW_READ_FIELD       = "_allowRead";
  public static final String ALLOW_UPDATE_FIELD     = "_allowUpdate";
  public static final String ALLOW_DELETE_FIELD     = "_allowDelete";
  public static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  public static final String ONCREATE_FIELD         = "onCreate.fields";

  public OUser create();

  public void load();

  public boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation);

  public OIdentifiable allowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName);

  public OIdentifiable allowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName);

  public OIdentifiable allowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId);

  public OIdentifiable disallowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName);

  public OIdentifiable disallowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName);

  public OIdentifiable disallowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId);

  public OUser authenticate(String iUsername, String iUserPassword);

  public OUser authenticate(final OToken authToken);

  public OUser getUser(String iUserName);

  public OUser getUser(final ORID iUserId);

  public OUser createUser(String iUserName, String iUserPassword, String... iRoles);

  public OUser createUser(String iUserName, String iUserPassword, ORole... iRoles);

  public boolean dropUser(String iUserName);

  public ORole getRole(String iRoleName);

  public ORole getRole(OIdentifiable role);

  public ORole createRole(String iRoleName, ORole.ALLOW_MODES iAllowMode);

  public ORole createRole(String iRoleName, ORole iParent, ORole.ALLOW_MODES iAllowMode);

  public boolean dropRole(String iRoleName);

  public List<ODocument> getAllUsers();

  public List<ODocument> getAllRoles();

  public void close(boolean onDelete);

  public void createClassTrigger();

  public OSecurity getUnderlying();

  public long getVersion();

  public void incrementVersion();
}
