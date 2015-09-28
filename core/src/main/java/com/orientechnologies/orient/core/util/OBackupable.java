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
package com.orientechnologies.orient.core.util;

import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Interface to claim the resource can be backed up ad restored.
 * 
 * @author Luca Garulli (l.garulli-at-orientechnologies.com)
 */
public interface OBackupable {
  /**
   * Executes a backup of the database. During the backup the database will be frozen in read-only mode.
   * 
   * @param out
   *          OutputStream used to write the backup content. Use a FileOutputStream to make the backup persistent on disk
   * @param options
   *          Backup options as Map<String, Object> object
   * @param callable
   *          Callback to execute when the database is locked
   * @param iListener
   *          Listener called for backup messages
   * @param compressionLevel
   *          ZIP Compression level between 1 (the minimum) and 9 (maximum). The bigger is the compression, the smaller will be the
   *          final backup content, but will consume more CPU and time to execute
   * @param bufferSize
   *          Buffer size in bytes, the bigger is the buffer, the more efficient will be the compression
   * @throws IOException
   * @see ODatabaseExport
   */
  List<String> backup(OutputStream out, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener, int compressionLevel, int bufferSize) throws IOException;

  /**
   * Executes a restore of a database backup. During the restore the database will be frozen in read-only mode.
   * 
   * @param in
   *          InputStream used to read the backup content. Use a FileInputStream to read a backup on a disk
   * @param options
   *          Backup options as Map<String, Object> object
   * @param callable
   *          Callback to execute when the database is locked
   * @param iListener
   *          Listener called for backup messages
   * @throws IOException
   * @see ODatabaseImport
   */
  void restore(InputStream in, Map<String, Object> options, Callable<Object> callable, OCommandOutputListener iListener)
      throws IOException;
}
