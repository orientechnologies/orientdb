/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under commercial license.
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

package com.orientechnologies.agent.hook;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OSoftThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.concurrent.BlockingQueue;

/**
 * Thread that log asynchronously.
 *
 * @author Luca Garulli
 */
public class OAuditingLoggingThread extends OSoftThread {
  private final String                   databaseURL;
  private final BlockingQueue<ODocument> auditingQueue;
  private ODatabaseDocumentTx            db;

  public OAuditingLoggingThread(final String iDatabaseURL, final String iDatabaseName, final BlockingQueue auditingQueue) {
    super(Orient.instance().getThreadGroup(), "OrientDB Auditing Logging Thread - " + iDatabaseName);

    setDumpExceptions(true);
    this.databaseURL = iDatabaseURL;
    this.auditingQueue = auditingQueue;
  }

  @Override
  public void startup() {
    db = new ODatabaseDocumentTx(databaseURL);
    db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);
    try {
      db.open("admin", "any");
    } catch (Exception e) {
      OLogManager.instance().error(this, "Cannot open database '%s'", e, databaseURL);
    }
  }

  @Override
  public void shutdown() {
    if (db != null)
      db.close();
  }

  @Override
  protected void execute() throws Exception {
    try {
      final ODocument log = auditingQueue.take();
      db.save(log);

    } catch (InterruptedException e) {
      // IGNORE AND SOFTLY EXIT
      interruptCurrentOperation();
    }
  }
}
