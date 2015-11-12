package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Attachment;
import com.orientechnologies.website.model.schema.dto.Issue;
import org.apache.commons.vfs2.FileObject;

import java.io.InputStream;
import java.util.List;

/**
 * Created by Enrico Risa on 06/11/15.
 */
public interface AttachmentRepository {

  List<Attachment> findIssueAttachment(String organization, Issue issue);

  Attachment attachToIssue(String owner, Issue issue, String name, InputStream inputStream);

  FileObject downloadAttachments(String owner, Issue issue, String fileName);

  void deleteAttachment(String organization, Issue issue, String fileName);

  void createIssueFolder(String organization, Issue issue);
}
