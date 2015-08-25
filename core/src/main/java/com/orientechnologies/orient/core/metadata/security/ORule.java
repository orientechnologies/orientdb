package com.orientechnologies.orient.core.metadata.security;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 08/11/14
 */
public class ORule implements Serializable{

  public static abstract class ResourceGeneric implements Serializable{
	private static final long serialVersionUID = 1L;
	private static final TreeMap<String, ResourceGeneric> nameToGenericMap = new TreeMap<String, ResourceGeneric>();
	private static final TreeMap<String, ResourceGeneric> legacyToGenericMap = new TreeMap<String, ResourceGeneric>();
	private static final Map<ResourceGeneric, String>     genericToLegacyMap = new HashMap<ResourceGeneric, String>();

    public static ResourceGeneric FUNCTION = new ResourceGeneric("FUNCTION", ODatabaseSecurityResources.FUNCTION){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric CLASS = new ResourceGeneric("CLASS", ODatabaseSecurityResources.CLASS){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric CLUSTER = new ResourceGeneric("CLUSTER", ODatabaseSecurityResources.CLUSTER){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric BYPASS_RESTRICTED = new ResourceGeneric("BYPASS_RESTRICTED", ODatabaseSecurityResources.BYPASS_RESTRICTED){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric DATABASE = new ResourceGeneric("DATABASE", ODatabaseSecurityResources.DATABASE){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric SCHEMA = new ResourceGeneric("SCHEMA", ODatabaseSecurityResources.SCHEMA){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric COMMAND = new ResourceGeneric("COMMAND", ODatabaseSecurityResources.COMMAND){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric RECORD_HOOK = new ResourceGeneric("RECORD_HOOK", ODatabaseSecurityResources.RECORD_HOOK){private static final long serialVersionUID = 1L;};
    public static ResourceGeneric SYSTEM_CLUSTERS = new ResourceGeneric("SYSTEM_CLUSTER", ODatabaseSecurityResources.SYSTEMCLUSTERS){private static final long serialVersionUID = 1L;};

    private final String name;
    private final String legacyName;
    protected ResourceGeneric(String name, String legacyName) {
    	this.name = name;
    	this.legacyName = legacyName!=null?legacyName:name;
    	register(this);
    }

    public String getName() {
		return name;
	}

	public String getLegacyName() {
		return legacyName;
	}

	private static void register(ResourceGeneric resource)
    {
		String legacyNameLowCase = resource.legacyName.toLowerCase();
		if(nameToGenericMap.containsKey(resource.name)
				|| legacyToGenericMap.containsKey(resource.legacyName.toLowerCase())
				|| genericToLegacyMap.containsKey(resource))
		{
			throw new IllegalArgumentException(resource+" already registered");
		}
    	nameToGenericMap.put(resource.name, resource);
    	legacyToGenericMap.put(legacyNameLowCase, resource);
    	genericToLegacyMap.put(resource, resource.legacyName);
    }

    public static ResourceGeneric valueOf(String name)
    {
    	return nameToGenericMap.get(name);
    }

    public static ResourceGeneric[] values()
    {
    	return genericToLegacyMap.keySet().toArray(new ResourceGeneric[genericToLegacyMap.size()]);
    }

	@Override
	public String toString() {
		return ResourceGeneric.class.getSimpleName()+" [name="+name+", legacyName="+legacyName+"]";
	}

  }

  private static final long serialVersionUID  = 1L;

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
    Map.Entry<String, ResourceGeneric> found = ResourceGeneric.legacyToGenericMap.floorEntry(resource.toLowerCase());
    if (found == null)
      return null;

    if (resource.length() < found.getKey().length())
      return null;

    if (resource.substring(0, found.getKey().length()).equalsIgnoreCase(found.getKey()))
      return found.getValue();

    return null;
  }

  public static String mapResourceGenericToLegacyResource(ResourceGeneric resourceGeneric) {
    return ResourceGeneric.genericToLegacyMap.get(resourceGeneric);
  }

	public static String mapLegacyResourceToSpecificResource(String resource) {
		Map.Entry<String, ResourceGeneric> found = ResourceGeneric.legacyToGenericMap.floorEntry(resource.toLowerCase());

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
