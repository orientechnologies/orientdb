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

package com.orientechnologies.orient.incrementalbackup;

import com.jcraft.jsch.*;
import com.orientechnologies.backup.uploader.OLocalBackupUploader;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Test;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import static org.junit.Assert.assertTrue;

/**
 * It test the behaviour of the SFTP Uploader with the following scenario:
 *  - inserting 1000 triples in the db (set up)
 *  - inserting 1000 triples in the db
 *  - 1st incremental backup of the db
 *  - inserting 1000 triples in the db
 *  - 2nd incremental backup of the db
 *  - 1st backup upload on SFTP server
 *  - deleting on the SFTP server the first "incremental-backup" file
 *  - inserting 1000 triples in the db
 *  - 3rd incremental backup of the db
 *  - 2nd backup upload on SFTP server
 *  - checking the files in local folder and the S3 bucket
 */
public class SFTPUploaderTest extends AbstractUploaderTest {

  private final String               destinationDirectoryPath = "/orientdb-backups/";
  private final String               host                     = "206.142.241.244";
  private final String               port                     = "22";
  private final String               username                 = "root";
  private final String               password                 = "@ri3ntDBRocks";
  private       OLocalBackupUploader uploader                 = new OLocalBackupUploader("sftp");


  @Test
  public void testIncrementalBackup() {

    try {

      graph = new OrientGraphNoTx(this.dbURL);
      // insert
      this.banner("2nd op. - Inserting 5000 triples (10000 vertices, 5000 edges)");
      executeWrites(this.dbURL, 1000);

      // first backup
      this.banner("1st local backup");
      graph.getRawGraph().incrementalBackup(this.backupPath);
      System.out.println("Done.");

      // insert
      this.banner("3rd op. - Inserting 5000 triples (10000 vertices, 5000 edges)");
      executeWrites(this.dbURL, 1000);

      // second backup
      this.banner("2nd local backup");
      graph.getRawGraph().incrementalBackup(this.backupPath);
      System.out.println("Done.");

      // upload backup on AWS S3
      this.banner("1st backup upload on SFTP server");
      assertTrue(uploader.executeUpload(super.backupPath, this.destinationDirectoryPath, this.host, this.port, this.username, this.password));
      System.out.println("Done.");

      // deleting on S3 the second "incremental-backup" file
      this.banner("Deleting on the SFTP server the first \"incremental-backup\" file");
      int lastIndex = this.backupPath.length()-1;
      String backupRelativePath;
      if(this.backupPath.charAt(lastIndex) == '/') {
        backupRelativePath = this.backupPath.substring(0, lastIndex);
      }
      else {
        backupRelativePath = this.backupPath;
      }
      String remoteFolderName = backupRelativePath.substring(backupRelativePath.lastIndexOf("/") + 1);

      File backupDirectory = new File(backupPath);
      File[] backupFiles = backupDirectory.listFiles();
      String fileName = remoteFolderName + "/" + backupFiles[backupFiles.length-1].getName();
      fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
      assertTrue(deleteFileOnSFTPServer(this.destinationDirectoryPath, fileName));
      System.out.println("Done.");

      // insert
      this.banner("4th op. - Inserting 5000 triples (10000 vertices, 5000 edges)");
      executeWrites(this.dbURL, 1000);

      // third backup
      this.banner("3rd local backup");
      graph.getRawGraph().incrementalBackup(this.backupPath);
      System.out.println("Done.");

      // upload backup on AWS S3
      this.banner("2nd backup upload on SFTP server");
      assertTrue(uploader.executeUpload(super.backupPath, this.destinationDirectoryPath, this.host, this.port, this.username, this.password));
      System.out.println("Done.");

      // checking consistency between local and remote backups
      this.banner("Checking consistency between local and remote backups");
      assertTrue(this.checkConsistency());
      System.out.println("Done.");

    } finally {
      this.graph.shutdown();
      ODatabaseRecordThreadLocal.INSTANCE.set(null);
      // cleaning all the directories
      this.cleanDirectories();
    }

  }

  private boolean checkConsistency() {

    boolean consistent = true;
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

      // browsing remote path
      String[] folders = destinationDirectoryPath.split( "/" );
      for ( String folder : folders ) {
        if ( folder.length() > 0 ) {
          sftp.cd(folder);
        }
      }

      List<String> remoteFileNames = new LinkedList<String>();
      Vector remoteFiles = sftp.ls("*");
      for(Object entry: remoteFiles) {
        remoteFileNames.add(((ChannelSftp.LsEntry)entry).getFilename());
      }

      File localBackupDirectory = new File(this.backupPath);
      File[] filesLocalBackup = localBackupDirectory.listFiles();
      List<String> localFileNames = new LinkedList<String>();
      for(File f: filesLocalBackup) {
        localFileNames.add(f.getName());
      }

      // comparing files
      for(String fileName: localFileNames) {
        if(!remoteFileNames.contains(fileName)) {
          consistent = false;
          break;
        }
      }

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

    return consistent;
  }

  private boolean deleteFileOnSFTPServer(String destinationDirectoryPath, String fileName) {

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

      // browsing remote path
      String[] folders = destinationDirectoryPath.split( "/" );
      for ( String folder : folders ) {
        if ( folder.length() > 0 ) {
            sftp.cd(folder);
        }
      }

      sftp.rm(fileName);

      success = true;

    } catch (JSchException e) {
      e.printStackTrace();
    } catch (SftpException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
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
  protected String getDatabaseName() {
    return "db-upload-sftp";
  }

}
