/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.sql;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityRole;
import org.junit.Test;

/** Created by Enrico Risa on 07/06/16. */
public class OCommandExecutorSQLGrantRevokeTest {

  @Test
  public void grantServerRemove() {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:grant");
    try {
      db.create();

      ORole testRole =
          db.getMetadata()
              .getSecurity()
              .createRole("testRole", OSecurityRole.ALLOW_MODES.DENY_ALL_BUT);

      assertFalse(testRole.allow(ORule.ResourceGeneric.SERVER, "server", ORole.PERMISSION_EXECUTE));

      db.command(new OCommandSQL("GRANT execute on server.remove to testRole")).execute();

      testRole = db.getMetadata().getSecurity().getRole("testRole");

      assertTrue(testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));

      db.command(new OCommandSQL("REVOKE execute on server.remove from testRole")).execute();

      testRole = db.getMetadata().getSecurity().getRole("testRole");

      assertFalse(testRole.allow(ORule.ResourceGeneric.SERVER, "remove", ORole.PERMISSION_EXECUTE));

    } finally {
      db.drop();
    }
  }
}
