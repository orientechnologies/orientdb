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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.List;
import java.util.Set;

/**
 * Manages users and roles.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public interface OSecurity {
  static final String RESTRICTED_CLASSNAME = "ORestricted";
  @Deprecated static final String IDENTITY_CLASSNAME = OIdentity.CLASS_NAME;
  static final String ALLOW_ALL_FIELD = "_allow";
  static final String ALLOW_READ_FIELD = "_allowRead";
  static final String ALLOW_UPDATE_FIELD = "_allowUpdate";
  static final String ALLOW_DELETE_FIELD = "_allowDelete";
  static final String ONCREATE_IDENTITY_TYPE = "onCreate.identityType";
  static final String ONCREATE_FIELD = "onCreate.fields";

  @Deprecated
  boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation);

  /**
   * Record level security: allows a user to access to a record.
   *
   * @param iDocument ODocument instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *     <ul>
   *       <li>ALLOW_ALL, to provide full access (RUD)
   *       <li>ALLOW_READ, to provide read access
   *       <li>ALLOW_UPDATE, to provide update access
   *       <li>ALLOW_DELETE, to provide delete access
   *     </ul>
   *
   * @param iUserName User name to provide the access
   * @return The OIdentity instance allowed
   */
  @Deprecated
  OIdentifiable allowUser(
      final ODocument iDocument, final ORestrictedOperation iOperationType, final String iUserName);

  /**
   * Record level security: allows a role to access to a record.
   *
   * @param iDocument ODocument instance to give access
   * @param iOperationType Operation type to use based on the permission to allow:
   *     <ul>
   *       <li>ALLOW_ALL, to provide full access (RUD)
   *       <li>ALLOW_READ, to provide read access
   *       <li>ALLOW_UPDATE, to provide update access
   *       <li>ALLOW_DELETE, to provide delete access
   *     </ul>
   *
   * @param iRoleName Role name to provide the access
   * @return The OIdentity instance allowed
   */
  @Deprecated
  OIdentifiable allowRole(
      final ODocument iDocument, final ORestrictedOperation iOperationType, final String iRoleName);

  /**
   * Record level security: deny a user to access to a record.
   *
   * @param iDocument ODocument instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *     <ul>
   *       <li>ALLOW_ALL, to provide full access (RUD)
   *       <li>ALLOW_READ, to provide read access
   *       <li>ALLOW_UPDATE, to provide update access
   *       <li>ALLOW_DELETE, to provide delete access
   *     </ul>
   *
   * @param iUserName User name to deny the access
   * @return The OIdentity instance denied
   */
  @Deprecated
  OIdentifiable denyUser(
      final ODocument iDocument, final ORestrictedOperation iOperationType, final String iUserName);

  /**
   * Record level security: deny a role to access to a record.
   *
   * @param iDocument ODocument instance to give access
   * @param iOperationType Operation type to use based on the permission to deny:
   *     <ul>
   *       <li>ALLOW_ALL, to provide full access (RUD)
   *       <li>ALLOW_READ, to provide read access
   *       <li>ALLOW_UPDATE, to provide update access
   *       <li>ALLOW_DELETE, to provide delete access
   *     </ul>
   *
   * @param iRoleName Role name to deny the access
   * @return The OIdentity instance denied
   */
  @Deprecated
  OIdentifiable denyRole(
      final ODocument iDocument, final ORestrictedOperation iOperationType, final String iRoleName);

  @Deprecated
  OUser authenticate(String iUsername, String iUserPassword);

  @Deprecated
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
}
