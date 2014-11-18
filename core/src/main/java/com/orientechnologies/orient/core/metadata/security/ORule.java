package com.orientechnologies.orient.core.metadata.security;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 08/11/14
 */
public class ORule implements Serializable{

  public enum ResourceGeneric {
    FUNCTION, CLASS, CLUSTER, BYPASS_RESTRICTED, DATABASE, SCHEMA, COMMAND, RECORD_HOOK
  }

  private static final long serialVersionUID  = 1L;
  
  private static final TreeMap<String, ResourceGeneric> legacyToGenericMap;
  private static final Map<ResourceGeneric, String>     genericToLegacyMap = new HashMap<ResourceGeneric, String>();

  static {
    legacyToGenericMap = new TreeMap<String, ResourceGeneric>();
    legacyToGenericMap.put(ODatabaseSecurityResources.CLASS.toLowerCase(), ResourceGeneric.CLASS);
    legacyToGenericMap.put(ODatabaseSecurityResources.BYPASS_RESTRICTED.toLowerCase(), ResourceGeneric.BYPASS_RESTRICTED);
    legacyToGenericMap.put(ODatabaseSecurityResources.CLUSTER.toLowerCase(), ResourceGeneric.CLUSTER);
    legacyToGenericMap.put(ODatabaseSecurityResources.COMMAND.toLowerCase(), ResourceGeneric.COMMAND);
    legacyToGenericMap.put(ODatabaseSecurityResources.DATABASE.toLowerCase(), ResourceGeneric.DATABASE);
    legacyToGenericMap.put(ODatabaseSecurityResources.FUNCTION.toLowerCase(), ResourceGeneric.FUNCTION);
    legacyToGenericMap.put(ODatabaseSecurityResources.RECORD_HOOK.toLowerCase(), ResourceGeneric.RECORD_HOOK);
    legacyToGenericMap.put(ODatabaseSecurityResources.SCHEMA.toLowerCase(), ResourceGeneric.SCHEMA);

    genericToLegacyMap.put(ResourceGeneric.CLASS, ODatabaseSecurityResources.CLASS);
    genericToLegacyMap.put(ResourceGeneric.BYPASS_RESTRICTED, ODatabaseSecurityResources.BYPASS_RESTRICTED);
    genericToLegacyMap.put(ResourceGeneric.CLUSTER, ODatabaseSecurityResources.CLUSTER);
    genericToLegacyMap.put(ResourceGeneric.COMMAND, ODatabaseSecurityResources.COMMAND);
    genericToLegacyMap.put(ResourceGeneric.DATABASE, ODatabaseSecurityResources.DATABASE);
    genericToLegacyMap.put(ResourceGeneric.FUNCTION, ODatabaseSecurityResources.FUNCTION);
    genericToLegacyMap.put(ResourceGeneric.RECORD_HOOK, ODatabaseSecurityResources.RECORD_HOOK);
    genericToLegacyMap.put(ResourceGeneric.SCHEMA, ODatabaseSecurityResources.SCHEMA);
  }

  private final ResourceGeneric                         resourceGeneric;
  private final Map<String, Byte>                       specificResources  = new HashMap<String, Byte>();

  private Byte                                          access             = null;

  public ORule(ResourceGeneric resourceGeneric, Map<String, Byte> specificResources, Byte access) {
    this.resourceGeneric = resourceGeneric;
    if (specificResources != null)
      this.specificResources.putAll(specificResources);
    this.access = access;
  }

  public static ResourceGeneric mapLegacyResourceToGenericResource(String resource) {
    Map.Entry<String, ResourceGeneric> found = legacyToGenericMap.floorEntry(resource.toLowerCase());
    if (found == null)
      return null;

    if (resource.length() < found.getKey().length())
      return null;

    if (resource.substring(0, found.getKey().length()).equalsIgnoreCase(found.getKey()))
      return found.getValue();

    return null;
  }

  public static String mapResourceGenericToLegacyResource(ResourceGeneric resourceGeneric) {
    return genericToLegacyMap.get(resourceGeneric);
  }

	public static String mapLegacyResourceToSpecificResource(String resource) {
		Map.Entry<String, ResourceGeneric> found = legacyToGenericMap.floorEntry(resource.toLowerCase());

		if (found == null)
			return resource;

		if (resource.length() < found.getKey().length())
			return resource;

		if (resource.length() == found.getKey().length())
			return null;

		if (resource.substring(0, found.getKey().length()).equalsIgnoreCase(found.getKey()))
			return resource.substring(found.getKey().length() + 1);

		return resource;
	}

  public Byte getAccess() {
    return access;
  }

  public ResourceGeneric getResourceGeneric() {
    return resourceGeneric;
  }

  public Map<String, Byte> getSpecificResources() {
    return specificResources;
  }

  public void grantAccess(String resource, int operation) {
    if (resource == null)
      access = grant((byte) operation, access);
    else {
      resource = resource.toLowerCase();
      Byte ac = specificResources.get(resource);
      specificResources.put(resource, grant((byte) operation, ac));
    }
  }

  private byte grant(byte operation, Byte ac) {
    byte currentValue = ac == null ? ORole.PERMISSION_NONE : ac;

    currentValue |= (byte) operation;
    return currentValue;
  }

  public void revokeAccess(String resource, int operation) {
    if (operation == ORole.PERMISSION_NONE)
      return;

    if (resource == null)
      access = revoke((byte) operation, access);
    else {
      resource = resource.toLowerCase();
      Byte ac = specificResources.get(resource);
      specificResources.put(resource, revoke((byte) operation, ac));
    }
  }

  private byte revoke(byte operation, Byte ac) {
    byte currentValue;
    if (ac == null)
      currentValue = ORole.PERMISSION_NONE;
    else {
      currentValue = ac.byteValue();
      currentValue &= ~(byte) operation;
    }
    return currentValue;
  }

  public Boolean isAllowed(String name, int operation) {
    if (name == null)
      return allowed((byte) operation, access);

    if (specificResources.isEmpty())
      return isAllowed(null, operation);

    final Byte ac = specificResources.get(name.toLowerCase());
    final Boolean allowed = allowed((byte) operation, ac);
    if (allowed == null)
      return isAllowed(null, operation);

    return allowed;
  }

  private Boolean allowed(byte operation, Byte ac) {
    if (ac == null)
      return null;

    final byte mask = (byte) operation;

    return (ac.byteValue() & mask) == mask;
  }

  public boolean containsSpecificResource(String resource) {
    if (specificResources.isEmpty())
      return false;

    return specificResources.containsKey(resource.toLowerCase());
  }
}
