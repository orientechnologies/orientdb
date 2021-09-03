package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.parser.OSecurityResourceSegment;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class OSecurityResource {

  private static Map<String, OSecurityResource> cache = new ConcurrentHashMap<>();

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
    } else if (resource.equals("database")) {
      return OSecurityResourceDatabaseOp.DB;
    } else if (resource.equals("database.create")) {
      return OSecurityResourceDatabaseOp.CREATE;
    } else if (resource.equals("database.copy")) {
      return OSecurityResourceDatabaseOp.COPY;
    } else if (resource.equals("database.drop")) {
      return OSecurityResourceDatabaseOp.DROP;
    } else if (resource.equals("database.exists")) {
      return OSecurityResourceDatabaseOp.EXISTS;
    } else if (resource.equals("database.command")) {
      return OSecurityResourceDatabaseOp.COMMAND;
    } else if (resource.equals("database.command.gremlin")) {
      return OSecurityResourceDatabaseOp.COMMAND_GREMLIN;
    } else if (resource.equals("database.freeze")) {
      return OSecurityResourceDatabaseOp.FREEZE;
    } else if (resource.equals("database.release")) {
      return OSecurityResourceDatabaseOp.RELEASE;
    } else if (resource.equals("database.passthrough")) {
      return OSecurityResourceDatabaseOp.PASS_THROUGH;
    } else if (resource.equals("database.bypassRestricted")) {
      return OSecurityResourceDatabaseOp.BYPASS_RESTRICTED;
    } else if (resource.equals("database.hook.record")) {
      return OSecurityResourceDatabaseOp.HOOK_RECORD;
    } else if (resource.equals("server")) {
      return OSecurityResourceServerOp.SERVER;
    } else if (resource.equals("server.status")) {
      return OSecurityResourceServerOp.STATUS;
    } else if (resource.equals("server.remove")) {
      return OSecurityResourceServerOp.REMOVE;
    } else if (resource.equals("server.admin")) {
      return OSecurityResourceServerOp.ADMIN;
    }
    try {
      OSecurityResourceSegment parsed = OSQLEngine.parseSecurityResource(resource);

      if (resource.startsWith("database.class.")) {
        OSecurityResourceSegment classElement = parsed.getNext().getNext();
        String className;
        boolean allClasses = false;
        if (classElement.getIdentifier() != null) {
          className = classElement.getIdentifier().getStringValue();
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
      } else if (resource.startsWith("database.systemclusters.")) {
        OSecurityResourceSegment clusterElement = parsed.getNext().getNext();
        String clusterName = clusterElement.getIdentifier().getStringValue();
        if (clusterElement.getNext() != null) {
          throw new OSecurityException("Invalid resource: " + resource);
        }
        return new OSecurityResourceCluster(resource, clusterName);
      }

      throw new OSecurityException("Invalid resource: " + resource);
    } catch (Exception ex) {
      ex.printStackTrace();
      throw new OSecurityException("Invalid resource: " + resource);
    }
  }

  protected final String resourceString;

  public OSecurityResource(String resourceString) {
    this.resourceString = resourceString;
  }

  public String getResourceString() {
    return resourceString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OSecurityResource that = (OSecurityResource) o;
    return Objects.equals(resourceString, that.resourceString);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceString);
  }
}
