package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.configuration.FSConfiguration;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.filesystem.PathResolver;
import com.orientechnologies.website.model.schema.dto.Attachment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.AttachmentRepository;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Enrico Risa on 06/11/15.
 */
@Component
public class AttachmentRepositoryImpl implements AttachmentRepository {

  @Autowired
  protected PathResolver    resolver;

  @Autowired
  protected FSConfiguration configuration;

  @Override
  public List<Attachment> findIssueAttachment(String organization, Issue issue) {

    FileSystemManager fsManager;

    try {
      fsManager = resolver.creteFileSystemManager(configuration);
      List<Attachment> attachments = new ArrayList<>();
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue, null));
      if (dir.exists() && dir.getType().equals(FileType.FOLDER)) {
        FileObject[] children = dir.getChildren();
        for (FileObject child : children) {
          Attachment attachment = getAttachment(child);
          attachments.add(attachment);
        }
      }
      return attachments;
    } catch (IOException e) {
      throw ServiceException.create(12, "Error");
    }
  }

  private Attachment getAttachment(FileObject child) throws FileSystemException {
    Attachment attachment = new Attachment();
    attachment.setName(child.getName().getBaseName());
    attachment.setSize(child.getContent().getSize());
    attachment.setType(child.getName().getExtension());
    attachment.setmTime(child.getContent().getLastModifiedTime());
    return attachment;
  }

  // TODO FILE UPLOAD
  @Override
  public Attachment attachToIssue(String organization, Issue issue, String name, InputStream inputStream) {

    FileSystemManager fsManager;
    FileObject localFile;
    FileObject remoteFile;
    try {
      fsManager = resolver.creteFileSystemManager(configuration);

      localFile = fsManager.resolveFile("ram:/tmp/" + UUID.randomUUID().toString());

      localFile.createFile();
      OutputStream outputStream = localFile.getContent().getOutputStream();
      IOUtils.copy(inputStream, outputStream);
      outputStream.flush();
      remoteFile = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue, name));
      remoteFile.copyFrom(localFile, Selectors.SELECT_SELF);

      Attachment attachment = getAttachment(remoteFile);
      localFile.close();
      remoteFile.close();

      return attachment;

    } catch (IOException e) {
      e.printStackTrace();
    } finally {

    }
    return null;
  }

  @Override
  public FileObject downloadAttachments(String organization, Issue issue, String fileName) {

    FileSystemManager fsManager;
    try {
      fsManager = resolver.creteFileSystemManager(configuration);
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue, fileName));

      if (dir.exists()) {
        return dir;
      }
      return null;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public void deleteAttachment(String organization, Issue issue, String fileName) {

    FileSystemManager fsManager;
    try {
      fsManager = resolver.creteFileSystemManager(configuration);
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue, fileName));
      if (dir.exists()) {
        dir.delete();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void createIssueFolder(String organization, Issue issue) {

    FileSystemManager fsManager;
    try {
      fsManager = resolver.creteFileSystemManager(configuration);
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue, null));
      if (!dir.exists()) {
        dir.createFolder();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
