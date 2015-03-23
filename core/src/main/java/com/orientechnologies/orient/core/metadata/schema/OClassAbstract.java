package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;
import java.util.Map;

public abstract class OClassAbstract implements OClass {
	private String name;
	private String shortName;
	private Class<?> javaClass;
	private List<OClass> superClasses;
	private boolean strictMode = false;
	private boolean abstractClass = false;
	private Map<String, OProperty> properties;
	

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OClass setName(String iName) {
		this.name = iName;
		return this;
	}

	@Override
	public Class<?> getJavaClass() {
		return javaClass;
	}

	@Override
	public String getShortName() {
		return shortName;
	}

	@Override
	public OClass setShortName(String shortName) {
		this.shortName = shortName;
		return this;
	}

	@Override
	public boolean isAbstract() {
		return abstractClass;
	}

	@Override
	public OClass setAbstract(boolean iAbstract) {
		this.abstractClass = iAbstract;
		return this;
	}

	@Override
	public boolean isStrictMode() {
		return strictMode;
	}

	@Override
	public OClass setStrictMode(boolean iMode) {
		this.strictMode = iMode;
		return this;
	}
	
	@Override
	  public OProperty getProperty(String propertyName) {

	    propertyName = propertyName.toLowerCase();
	    
	    OProperty p = properties.get(propertyName);
	    if(p!=null) return p;
	    for(int i=0;i<superClasses.size() && p==null;i++)
	    {
	    	p = superClasses.get(i).getProperty(propertyName);
	    }
	    return p;
	  }
	
	@Override
	  public boolean isSubClassOf(String iClassName) {
	    if (iClassName == null) return false;
	    
	    if(iClassName.equalsIgnoreCase(getName()) || iClassName.equalsIgnoreCase(getShortName())) return true;
	    for(OClass superClass: superClasses)
	    {
	    	if(superClass.isSubClassOf(iClassName)) return true;
	    }
	    return false;
	  }

	  @Override
	  public boolean isSubClassOf(OClass clazz) {
	    if (clazz == null) return false;
	    if(equals(clazz)) return true;
	    for(OClass superClass: superClasses)
	    {
	    	if(superClass.isSubClassOf(clazz)) return true;
	    }
	    return false;
	  }

	  @Override
	  public boolean isSuperClassOf(OClass clazz) {
	    return clazz != null && clazz.isSubClassOf(this);
	  }
	
	

}
