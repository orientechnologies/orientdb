package com.orientechnologies.website.filesystem;

import com.orientechnologies.website.configuration.FSConfiguration;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Issue;
import org.apache.commons.vfs2.FileSystemManager;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Enrico Risa on 06/11/15.
 */
@Component
public class PathResolverImpl implements PathResolver {

  Map<String, PathResolver> resolvers = new HashMap<>();

  @PostConstruct
  protected void initResolvers() {

    resolvers.put("local", new LocalPathResolver());
  }

  @Override
  public String resolvePath(FSConfiguration configuration, String organization, Issue issue) {
    PathResolver pathResolver = resolvers.get(configuration.protocol);
    if (pathResolver == null) {
      throw ServiceException.create(10, "Cannot find resolver for protocol: " + configuration.protocol);
    }
    return pathResolver.resolvePath(configuration, organization, issue);
  }

  @Override
  public FileSystemManager creteFileSystemManager(FSConfiguration configuration) throws IOException {
    PathResolver pathResolver = resolvers.get(configuration.protocol);
    if (pathResolver == null) {
      throw ServiceException.create(10, "Cannot find resolver for protocol: " + configuration.protocol);
    }
    return pathResolver.creteFileSystemManager(configuration);
  }
}
