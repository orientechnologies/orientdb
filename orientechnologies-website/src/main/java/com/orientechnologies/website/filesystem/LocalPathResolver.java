package com.orientechnologies.website.filesystem;

import com.orientechnologies.website.configuration.FSConfiguration;
import com.orientechnologies.website.model.schema.dto.Issue;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.VFS;

import java.io.IOException;

/**
 * Created by Enrico Risa on 06/11/15.
 */
public class LocalPathResolver implements PathResolver {

  @Override
  public String resolvePath(FSConfiguration configuration, String organization, Issue issue) {
    return "file://" + configuration.basePath + "/" + organization + "/" + issue.getClient().getClientId() + "/" + issue.getIid();
  }

  @Override
  public FileSystemManager creteFileSystemManager(FSConfiguration configuration) throws IOException {
    FileSystem fs = null;
    FileSystemOptions opts = new FileSystemOptions();
    return VFS.getManager();
  }
}
