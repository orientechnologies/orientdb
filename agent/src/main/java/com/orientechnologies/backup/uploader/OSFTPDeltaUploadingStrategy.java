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
import com.orientechnologies.agent.Utils;
import com.orientechnologies.agent.services.backup.log.OBackupUploadFinishedLog;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This strategy performs an upload to a SFTP server. The upload of the delta between the local backup directory and the remote one
 * is performed.
 */

public class OSFTPDeltaUploadingStrategy implements OUploadingStrategy {

  private String  host;
  private Integer port;
  private String  username;
  private String  password;
  private String  key;
  private String  path;

  public OSFTPDeltaUploadingStrategy() {
  }

  //

  /**
   * Uploads a backup to a SFTP server
   *
   * @param sourceBackupDirectory
   * @param destinationDirectoryPath
   * @param accessParameters         (String host, String port, String username, String password)
   *
   * @return success
   */
  public boolean executeUpload(String sourceBackupDirectory, String destinationDirectoryPath, String... accessParameters) {

    boolean success = false;
    Session session = null;
    Channel channel = null;
    ChannelSftp channelSftp = null;

    try {

      // SFTP connection
      JSch ssh = new JSch();
      session = ssh.getSession(username, host, port);
      session.setPassword(password);
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp sftp = (ChannelSftp) channel;

      // browsing remote path, and if a directory doesn't exist it will be created
      String[] folders = destinationDirectoryPath.split("/");
      for (String folder : folders) {
        if (folder.length() > 0) {
          try {
            sftp.cd(folder);
          } catch (SftpException e) {
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
      Map<String, File> localFileName2File = new ConcurrentHashMap<String, File>();
      for (File f : filesLocalBackup) {
        localFileName2File.put(f.getName(), f);
      }

      List<String> remoteFileNames = new LinkedList<String>();
      Vector remoteFiles = sftp.ls("*");
      for (Object entry : remoteFiles) {
        remoteFileNames.add(((ChannelSftp.LsEntry) entry).getFilename());
      }

      // compare files in the bucket with the local ones and populate filesToUpload list
      for (String fileName : localFileName2File.keySet()) {
        if (remoteFileNames.contains(fileName)) {
          localFileName2File.remove(fileName);
        }
      }

      // upload each file contained in the filesToUpload list
      for (File currentFile : localFileName2File.values()) {
        FileInputStream src = new FileInputStream(currentFile);
        try {
          sftp.put(src, currentFile.getName());
        } finally {
          Utils.safeClose(this, src);
        }
      }

      success = true;

    } catch (JSchException e) {
      OLogManager.instance().info(this, "Caught a JSchException.");
      OLogManager.instance().info(this, "Error Message:    %s", e.getMessage());
    } catch (SftpException e) {
      OLogManager.instance().info(this, "Caught a SftpException.");
      OLogManager.instance().info(this, "Error Message:    %s", e.getMessage());
    } catch (Exception e) {
      OLogManager.instance().info(this, "Caught a Exception.");
      OLogManager.instance().info(this, "Error Message:    %s", e.getMessage());
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

  @Override
  public OUploadMetadata executeUpload(String sourceFile, String fName, String destinationDirectoryPath) {

    boolean success = false;
    Session session = null;
    Channel channel = null;
    ChannelSftp channelSftp = null;

    Map<String, String> metadata = new HashMap<>();
    metadata.putIfAbsent("directory", destinationDirectoryPath);

    long start = System.currentTimeMillis();

    try {
      // SFTP connection
      JSch ssh = new JSch();
      session = ssh.getSession(username, host, port);
      session.setPassword(password);
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp sftp = (ChannelSftp) channel;

      String dest = path + File.separator + destinationDirectoryPath;

      // browsing remote path, and if a directory doesn't exist it will be created
      String[] folders = dest.split("/");
      for (String folder : folders) {
        try {
          sftp.cd(folder.isEmpty() ? "/" : folder);
        } catch (SftpException e) {
          sftp.mkdir(folder);
          sftp.cd(folder);
        }
      }

      File file = new File(sourceFile);
      FileInputStream src = new FileInputStream(file);
      try {
        sftp.put(src, fName);
      } finally {
        Utils.safeClose(this, src);
      }

    } catch (Exception e) {
      OLogManager.instance().error(this, "Error", e);
    }

    long end = System.currentTimeMillis();
    long elapsed = end - start;

    return new OUploadMetadata("sftp", elapsed, metadata);
  }

  @Override
  public void config(ODocument cfg) {

    host = cfg.field("host");
    port = cfg.field("port");
    username = cfg.field("username");
    password = System.getenv("BACKUP_SFTP_PASSWORD");
    path = cfg.field("path");
    key = System.getenv("BACKUP_SFTP_KEY");

  }

  @Override
  public String executeDownload(OBackupUploadFinishedLog upload) {

    boolean success = false;
    Session session = null;
    Channel channel = null;
    ChannelSftp channelSftp = null;
    try {
      // SFTP connection
      Map<String, String> metadata = upload.getMetadata();
      JSch ssh = new JSch();
      session = ssh.getSession(username, host, port);
      if (password != null) {
        session.setPassword(password);
      }
      if (key != null) {
        ssh.addIdentity(key);
      }
      java.util.Properties config = new java.util.Properties();
      config.put("StrictHostKeyChecking", "no");
      session.setConfig(config);
      session.connect();
      channel = session.openChannel("sftp");
      channel.connect();
      ChannelSftp sftp = (ChannelSftp) channel;

      String directory = metadata.get("directory");

      String dest = path + File.separator + directory;

      sftp.cd(dest);

      Vector<ChannelSftp.LsEntry> ls = sftp.ls(".");

      Path tempDir = Files.createTempDirectory(directory);
      for (ChannelSftp.LsEntry l : ls) {
        if (!l.getFilename().equalsIgnoreCase(".") && !l.getFilename().equalsIgnoreCase("..")) {
          Files.copy(sftp.get(l.getFilename()), tempDir.resolve(l.getFilename()));
        }
      }

      return tempDir.toString();
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error " + e.getMessage(), e);
    }
    return null;
  }

}
