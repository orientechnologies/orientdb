/*
 * Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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

package com.orientechnologies.backup.uploader;

import com.jcraft.jsch.*;
import com.orientechnologies.common.log.OLogManager;

import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This strategy performs an upload to a SFTP server. The upload of the delta between the local backup directory and the remote one is performed.
 */

public class OSFTPDeltaUploadingStrategy implements OUploadingStrategy {

  public OSFTPDeltaUploadingStrategy() {}


  //

  /**
   * Uploads a backup to a SFTP server
   *
   * @param sourceBackupDirectory
   * @param destinationDirectoryPath
   * @param accessParameters (String host, String port, String username, String password)
   * @return success
   */
  public boolean executeUpload(String sourceBackupDirectory, String destinationDirectoryPath, String... accessParameters) {

    String host = accessParameters[0];
    String port = accessParameters[1];
    String username = accessParameters[2];
    String password = accessParameters[3];

    boolean success = false;
    Session session = null;
    Channel channel = null;
    ChannelSftp channelSftp = null;

    try {

      // SFTP connection
      JSch ssh = new JSch();
      session = ssh.getSession(username, host, Integer.parseInt(port));
      session.setPassword(password);
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp sftp = (ChannelSftp) channel;

      // browsing remote path, and if a directory doesn't exist it will be created
      String[] folders = destinationDirectoryPath.split( "/" );
      for ( String folder : folders ) {
        if ( folder.length() > 0 ) {
          try {
            sftp.cd(folder);
          }
          catch ( SftpException e ) {
            sftp.mkdir(folder);
            sftp.cd(folder);
          }
        }
      }

    /*
     * uploading file to the bucket
     */

      File localBackupDirectory = new File(sourceBackupDirectory);

      File[] filesLocalBackup = localBackupDirectory.listFiles();
      Map<String, File> localFileName2File = new ConcurrentHashMap<String,File>();
      for(File f: filesLocalBackup) {
        localFileName2File.put(f.getName(), f);
      }

      List<String> remoteFileNames = new LinkedList<String>();
      Vector remoteFiles = sftp.ls("*");
      for(Object entry: remoteFiles) {
        remoteFileNames.add(((ChannelSftp.LsEntry)entry).getFilename());
      }

      // compare files in the bucket with the local ones and populate filesToUpload list
      for(String fileName: localFileName2File.keySet()) {
        if(remoteFileNames.contains(fileName)) {
          localFileName2File.remove(fileName);
        }
      }

      // upload each file contained in the filesToUpload list
      for (File currentFile: localFileName2File.values()) {
        sftp.put(new FileInputStream(currentFile), currentFile.getName());
      }

      success =  true;

    } catch (JSchException e) {
      OLogManager.instance().info(this,"Caught a JSchException.");
      OLogManager.instance().info(this,"Error Message:    %s", e.getMessage());
    } catch (SftpException e) {
      OLogManager.instance().info(this,"Caught a SftpException.");
      OLogManager.instance().info(this,"Error Message:    %s", e.getMessage());
    } catch (Exception e) {
      OLogManager.instance().info(this,"Caught a Exception.");
      OLogManager.instance().info(this,"Error Message:    %s", e.getMessage());
    } finally {
      if (channel != null) {
        channel.disconnect();
      }
      if (session != null) {
        session.disconnect();
      }
    }

    return success;
  }
}
