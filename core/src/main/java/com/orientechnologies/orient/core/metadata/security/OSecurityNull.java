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

import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORole.ALLOW_MODES;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Dummy security implementation.
 * 
 * @author Luca Garulli
 * 
 */
public class OSecurityNull implements OSecurity {

  @Override
  public boolean isAllowed(final Set<OIdentifiable> iAllowAll, final Set<OIdentifiable> iAllowOperation) {
    return true;
  }

  public OUser create() {
    return null;
  }

  public void load() {
  }

  public OUser getUser(String iUserName) {
    return null;
  }

  public OUser createUser(String iUserName, String iUserPassword, String[] iRoles) {
    return null;
  }

  public ORole getRole(String iRoleName) {
    return null;
  }

  public ORole createRole(String iRoleName, ALLOW_MODES iAllowMode) {
    return null;
  }

  public ORole createRole(String iRoleName, ORole iParent, ALLOW_MODES iAllowMode) {
    return null;
  }

  public List<ODocument> getAllUsers() {
    return null;
  }

  public List<ODocument> getAllRoles() {
    return null;
  }

  public OUser authenticate(String iUsername, String iUserPassword) {
    return null;
  }

  public void close() {
  }

  public OUser repair() {
    return null;
  }

  public boolean dropUser(String iUserName) {
    return false;
  }

  public boolean dropRole(String iRoleName) {
    return false;
  }

  @Override
  public OIdentifiable allowUser(ODocument iDocument, String iAllowFieldName, String iUserName) {
    return null;
  }

  @Override
  public OIdentifiable allowRole(ODocument iDocument, String iAllowFieldName, String iRoleName) {
    return null;
  }

  @Override
  public OIdentifiable allowIdentity(ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return null;
  }

  @Override
  public OIdentifiable disallowUser(ODocument iDocument, String iAllowFieldName, String iUserName) {
    return null;
  }

  @Override
  public OIdentifiable disallowRole(ODocument iDocument, String iAllowFieldName, String iRoleName) {
    return null;
  }

  @Override
  public OIdentifiable disallowIdentity(ODocument iDocument, String iAllowFieldName, OIdentifiable iId) {
    return null;
  }
}
