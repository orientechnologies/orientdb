package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Attachment;
import com.orientechnologies.website.model.schema.dto.Issue;

import java.io.InputStream;
import java.util.List;

/**
 * Created by Enrico Risa on 06/11/15.
 */
public interface AttachmentRepository {

  List<Attachment> findIssueAttachment(String organization, Issue issue);

  void attachToIssue(String owner, Issue issue, String name, InputStream inputStream);
}
