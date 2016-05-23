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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.orientechnologies.backup.uploader.OLocalBackupUploader;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.junit.Test;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * It test the behaviour of the S3 Uploader with the following scenario:
 *  - inserting 1000 triples in the db (set up)
 *  - inserting 1000 triples in the db
 *  - 1st incremental backup of the db
 *  - inserting 1000 triples in the db
 *  - 2nd incremental backup of the db
 *  - 1st backup upload on S3 bucket
 *  - deleting on S3 the first "incremental-backup" file
 *  - inserting 1000 triples in the db
 *  - 3rd incremental backup of the db
 *  - 2nd backup upload on S3 bucket
 *  - checking the files in local folder and the S3 bucket
 */
public class S3UploaderTest extends AbstractUploaderTest {

  private final String               destinationDirectoryPath = "/orientdb-backups/";
  private final String               bucketName        = "orientdb-databases-backup";
  private final String               ACCESS_KEY        = "AKIAJLSQXFYNUAEP5EIA";
  private final String               SECRET_ACCESS_KEY = "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";
  private       OLocalBackupUploader uploader          = new OLocalBackupUploader("s3");


  @Test
  public void testIncrementalBackup() {

    try {

      this.graph = new OrientGraphNoTx(this.dbURL);
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
      this.banner("1st backup upload on S3");
      assertTrue(uploader.executeUpload(backupPath, this.destinationDirectoryPath, this.bucketName, this.ACCESS_KEY, this.SECRET_ACCESS_KEY));
      System.out.println("Done.");

      // deleting on S3 the second "incremental-backup" file
      this.banner("Deleting on S3 the first \"incremental-backup\" file");
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
      String keyName = remoteFolderName + "/" + backupFiles[backupFiles.length-1].getName();
      assertTrue(deleteFileOnS3(this.bucketName, keyName));
      System.out.println("Done.");

      // insert
      this.banner("4th op. - Inserting 5000 triples (10000 vertices, 5000 edges)");
      executeWrites(this.dbURL, 1000);

      // third backup
      this.banner("3rd local backup");
      graph.getRawGraph().incrementalBackup(this.backupPath);
      System.out.println("Done.");

      // upload backup on AWS S3
      this.banner("2nd backup upload on S3");
      assertTrue(uploader.executeUpload(backupPath, this.destinationDirectoryPath, this.bucketName, this.ACCESS_KEY, this.SECRET_ACCESS_KEY));
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

    try {

      AWSCredentials awsCredentials = new BasicAWSCredentials(this.ACCESS_KEY, this.SECRET_ACCESS_KEY);
      AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
      List<S3ObjectSummary> filesInBucket = s3client.listObjects(this.bucketName).getObjectSummaries();
      List<String> remoteFileNames = new LinkedList<String>();

      String currentRemoteFileName;
      for(S3ObjectSummary currentRemoteFile: filesInBucket) {
        currentRemoteFileName = currentRemoteFile.getKey();
        currentRemoteFileName = currentRemoteFileName.substring(currentRemoteFileName.lastIndexOf("/")+1);
        if(currentRemoteFileName.length() > 1) {
          remoteFileNames.add(currentRemoteFileName);
        }
      }

      File localBackupDirectory = new File(this.backupPath);
      File[] localBackupFiles = localBackupDirectory.listFiles();
      List<String> localFileNames = new LinkedList<String>();
      for(File currentLocalFile: localBackupFiles) {
        localFileNames.add(currentLocalFile.getName());
      }

      // comparing files
      for(String fileName: localFileNames) {
        if(!remoteFileNames.contains(fileName)) {
          consistent = false;
          break;
        }
      }

    } catch (AmazonServiceException ase) {
      consistent = false;
      OLogManager.instance().info(this,"Caught an AmazonServiceException, which " +
          "means your request made it " +
          "to Amazon S3, but was rejected with an error response" +
          " for some reason.");
      OLogManager.instance().info(this,"Error Message:    %s", ase.getMessage());
      OLogManager.instance().info(this,"HTTP Status Code: %s", ase.getStatusCode());
      OLogManager.instance().info(this,"AWS Error Code:   %s", ase.getErrorCode());
      OLogManager.instance().info(this,"Error Type:       %s", ase.getErrorType());
      OLogManager.instance().info(this,"Request ID:       %s", ase.getRequestId());
    } catch (AmazonClientException ace) {
      consistent = false;
      OLogManager.instance().info(this,"Caught an AmazonClientException, which " +
          "means the client encountered " +
          "an internal error while trying to " +
          "communicate with S3, " +
          "such as not being able to access the network.");
      OLogManager.instance().info(this,"Error Message: %s", ace.getMessage());
    } catch (Exception e) {
      consistent = false;
      OLogManager.instance().info(this,"Caught an exception client side.");
      OLogManager.instance().info(this,"Error Message: %s", e.getMessage());
    }

    return consistent;
  }

  private boolean deleteFileOnS3(String bucketName, String keyName) {

    boolean success = false;

    try {

      AWSCredentials awsCredentials = new BasicAWSCredentials(this.ACCESS_KEY, this.SECRET_ACCESS_KEY);
      AmazonS3Client s3client = new AmazonS3Client(awsCredentials);
      s3client.deleteObject(new DeleteObjectRequest(bucketName, keyName));

      success =  true;

    } catch (AmazonServiceException ase) {
      OLogManager.instance().info(this,"Caught an AmazonServiceException, which " +
          "means your request made it " +
          "to Amazon S3, but was rejected with an error response" +
          " for some reason.");
      OLogManager.instance().info(this,"Error Message:    %s", ase.getMessage());
      OLogManager.instance().info(this,"HTTP Status Code: %s", ase.getStatusCode());
      OLogManager.instance().info(this,"AWS Error Code:   %s", ase.getErrorCode());
      OLogManager.instance().info(this,"Error Type:       %s", ase.getErrorType());
      OLogManager.instance().info(this,"Request ID:       %s", ase.getRequestId());
    } catch (AmazonClientException ace) {
      OLogManager.instance().info(this,"Caught an AmazonClientException, which " +
          "means the client encountered " +
          "an internal error while trying to " +
          "communicate with S3, " +
          "such as not being able to access the network.");
      OLogManager.instance().info(this,"Error Message: %s", ace.getMessage());
    } catch (Exception e) {
      OLogManager.instance().info(this,"Caught an exception client side.");
      OLogManager.instance().info(this,"Error Message: %s", e.getMessage());
    }

    return success;

  }

  @Override
  protected String getDatabaseName() {
    return "db-upload-s3";
  }

}
