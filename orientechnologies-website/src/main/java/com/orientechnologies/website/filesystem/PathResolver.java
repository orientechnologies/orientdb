package com.orientechnologies.website.filesystem;

import com.orientechnologies.website.configuration.FSConfiguration;
import com.orientechnologies.website.model.schema.dto.Issue;
import org.apache.commons.vfs2.FileSystemManager;

import java.io.IOException;

/**
 * Created by Enrico Risa on 06/11/15.
 */

public interface PathResolver {

  public String resolvePath(FSConfiguration configuration, String organization, Issue issue);

  public FileSystemManager creteFileSystemManager(FSConfiguration configuration) throws IOException;
}
