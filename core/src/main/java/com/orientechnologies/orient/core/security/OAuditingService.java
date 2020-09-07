/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;

/**
 * Provides an interface the auditing service.
 *
 * @author S. Colin Leister
 */
public interface OAuditingService extends OSecurityComponent {
  void changeConfig(OSecurityUser user, final String databaseName, final ODocument cfg)
      throws IOException;

  ODocument getConfig(final String databaseName);

  void log(final OAuditingOperation operation, final String message);

  void log(final OAuditingOperation operation, OSecurityUser user, final String message);

  void log(
      final OAuditingOperation operation,
      final String dbName,
      OSecurityUser user,
      final String message);
}
