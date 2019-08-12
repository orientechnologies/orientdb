package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.Map;

public abstract class OSecurityResource {

  static Map<String, OSecurityResource> cache;

  public static OSecurityResource getInstance(String resource) {
    OSecurityResource result = cache.get(resource);
    if (result == null) {
      result = parseResource(resource);
      if (result != null) {
        cache.put(resource, result);
      }
    }
    return result;
  }

  protected static OSecurityResource parseResource(String resource) {

    //TODO
//    public static final String DATABASE          = "database";
//    public static final String COMMAND           = "database.command";
//    public static final String COMMAND_GREMLIN   = "database.command.gremlin";
//    public static final String FUNCTION          = "database.function";
//    public static final String DATABASE_CONFIG   = "database.config";
//    public static final String BYPASS_RESTRICTED = "database.bypassRestricted";
//    public static final String RECORD_HOOK       = "database.hook.record";
//    public static final String SERVER_ADMIN      = "server.admin";

    if (resource.equals("*")) {
      return OSecurityResourceAll.INSTANCE;
    } else if (resource.equals("database.schema")) {
      return OSecurityResourceSchema.INSTANCE;
    } else if (resource.equals("database.class.*")) {
      return OSecurityResourceClass.ALL_CLASSES;
    } else if (resource.equals("database.class.*.*")) {
      return OSecurityResourceProperty.ALL_PROPERTIES;
    } else if (resource.equals("database.cluster.*")) {
      return OSecurityResourceCluster.ALL_CLUSTERS;
    } else if (resource.equals("database.systemclusters")) {
      return OSecurityResourceCluster.SYSTEM_CLUSTERS;
    } else if (resource.equals("database.function.*")) {
      return OSecurityResourceFunction.ALL_FUNCTIONS;
    }
    try {
      OSecurityResourceSegment parsed = OSQLEngine.parseSecurityResource(resource);

      if (resource.startsWith("database.class.")) {
        OSecurityResourceSegment classElement = parsed.getNext().getNext();
        String className;
        boolean allClasses = false;
        if (classElement.getIdentifier() != null) {
          className = classElement.getIdentifier().getStringValue();
        } else if (classElement.isCluster()) {
          className = "cluster";
        } else {
          className = null;
          allClasses = true;
        }
        OSecurityResourceSegment propertyModifier = classElement.getNext();
        if (propertyModifier != null) {
          if (propertyModifier.getNext() != null) {
            throw new OSecurityException("Invalid resource: " + resource);
          }
          String propertyName = propertyModifier.getIdentifier().getStringValue();
          if (allClasses) {
            return new OSecurityResourceProperty(resource, propertyName);
          } else {
            return new OSecurityResourceProperty(resource, className, propertyName);
          }
        } else {
          return new OSecurityResourceClass(resource, className);
        }
      } else if (resource.startsWith("database.cluster.")) {
        OSecurityResourceSegment clusterElement = parsed.getNext().getNext();
        String clusterName = clusterElement.getIdentifier().getStringValue();
        if (clusterElement.getNext() != null) {
          throw new OSecurityException("Invalid resource: " + resource);
        }
        return new OSecurityResourceCluster(resource, clusterName);
      } else if (resource.startsWith("database.function.")) {
        OSecurityResourceSegment functionElement = parsed.getNext().getNext();
        String functionName = functionElement.getIdentifier().getStringValue();
        if (functionElement.getNext() != null) {
          throw new OSecurityException("Invalid resource: " + resource);
        }
        return new OSecurityResourceFunction(resource, functionName);
      }
      //TODO

      throw new OSecurityException("Invalid resource: " + resource);
    } catch (Exception ex) {
      throw new OSecurityException("Invalid resource: " + resource);
    }
  }

  private final String resourceString;


  public OSecurityResource(String resourceString) {
    this.resourceString = resourceString;
  }

  public String getResourceString() {
    return resourceString;
  }

}
