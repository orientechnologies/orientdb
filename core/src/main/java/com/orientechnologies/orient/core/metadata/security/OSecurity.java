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
  static final String RESTRICTED_CLASSNAME   = "ORestricted";
  static final String IDENTITY_CLASSNAME     = "OIdentity";
  static final String ALLOW_ALL_FIELD        = "_allow";
  static final String ALLOW_READ_FIELD       = "_allowRead";
  static final String ALLOW_UPDATE_FIELD     = "_allowUpdate";
  static final String ALLOW_DELETE_FIELD     = "_allowDelete";
  static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  static final String ONCREATE_FIELD         = "onCreate.fields";

  OUser create();

  void load();

  boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation);

  OIdentifiable allowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName);

  OIdentifiable allowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName);

  OIdentifiable allowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId);

  OIdentifiable disallowUser(final ODocument iDocument, final String iAllowFieldName, final String iUserName);

  OIdentifiable disallowRole(final ODocument iDocument, final String iAllowFieldName, final String iRoleName);

  OIdentifiable disallowIdentity(final ODocument iDocument, final String iAllowFieldName, final OIdentifiable iId);

  OUser authenticate(String iUsername, String iUserPassword);

  OUser authenticate(final OToken authToken);

  OUser getUser(String iUserName);

  OUser getUser(final ORID iUserId);

  OUser createUser(String iUserName, String iUserPassword, String... iRoles);

  OUser createUser(String iUserName, String iUserPassword, ORole... iRoles);

  boolean dropUser(String iUserName);

  ORole getRole(String iRoleName);

  ORole getRole(OIdentifiable role);

  ORole createRole(String iRoleName, ORole.ALLOW_MODES iAllowMode);

  ORole createRole(String iRoleName, ORole iParent, ORole.ALLOW_MODES iAllowMode);

  boolean dropRole(String iRoleName);

  List<ODocument> getAllUsers();

  List<ODocument> getAllRoles();

  void close(boolean onDelete);

  void createClassTrigger();

  OSecurity getUnderlying();

  long getVersion();

  void incrementVersion();
}
