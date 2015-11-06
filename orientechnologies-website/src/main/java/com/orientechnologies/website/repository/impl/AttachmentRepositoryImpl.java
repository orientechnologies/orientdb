package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.configuration.FSConfiguration;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.filesystem.PathResolver;
import com.orientechnologies.website.model.schema.dto.Attachment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.AttachmentRepository;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

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
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue));
      if (dir.exists() && dir.getType().equals(FileType.FOLDER)) {
        FileObject[] children = dir.getChildren();
        for (FileObject child : children) {
          Attachment attachment = new Attachment();
          attachment.setName(child.getName().getBaseName());
          attachment.setSize(child.getContent().getSize());
          attachment.setType(child.getName().getExtension());
          attachments.add(attachment);
        }
      }
      return attachments;
    } catch (IOException e) {
      throw ServiceException.create(12, "Error");
    }
  }

  // TODO FILE UPLOAD
  @Override
  public void attachToIssue(String organization, Issue issue, String name, InputStream inputStream) {

    FileSystemManager fsManager;
    try {
      fsManager = resolver.creteFileSystemManager(configuration);
      FileObject dir = fsManager.resolveFile(resolver.resolvePath(configuration, organization, issue));

      if (dir.exists() && dir.getType().equals(FileType.FOLDER)) {

      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }
}
