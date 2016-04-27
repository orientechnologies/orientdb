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

package com.orientechnologies.agent.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Enrico Risa on 11/04/16.
 */
public class OBackupTxGroup {

  protected List<OBackupLog> logs = new LinkedList<OBackupLog>();

  public void push(OBackupLog log) {
    logs.add(0, log);
  }

  public List<ODocument> asDocs() {
    List<ODocument> docs = new ArrayList<ODocument>();
    for (OBackupLog log : logs) {
      docs.add(log.toDoc());
    }
    return docs;
  }
}
