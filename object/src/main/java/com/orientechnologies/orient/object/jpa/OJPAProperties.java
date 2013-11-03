package com.orientechnologies.orient.object.jpa;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * OrientDB settings. Configurable through com.google.inject.persist.jpa.JpaPersistModule.properties(Properties)
 */
final class OJPAProperties extends Properties {
	private static final long		serialVersionUID				= -8158054712863843518L;

	public final static String	URL											= "javax.persistence.jdbc.url";
	public final static String	USER										= "javax.persistence.jdbc.user";
	public final static String	PASSWORD								= "javax.persistence.jdbc.password";
	/**
	 * OrientDB specific
	 */
	public final static String	ENTITY_CLASSES_PACKAGE	= "com.orientdb.entityClasses";

	public OJPAProperties() {
	}

	/**
	 * Checks properties
	 * 
	 * @param properties
	 */
	public OJPAProperties(final Map<String, Object> properties) {
		putAll(properties);

		if (properties == null) {
			throw new IllegalStateException("Map properties for entity manager should not be null");
		}

		if (!checkContainsValue(URL)) {
			throw new IllegalStateException("URL propertiy for entity manager should not be null or empty");
		}

		if (!checkContainsValue(USER)) {
			throw new IllegalStateException("User propertiy for entity manager should not be null or empty");
		}
	}

	/**
	 * @return Unmodifiable Map of properties for use by the persistence provider.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Map<String, Object> getUnmodifiableProperties() {
		return Collections.unmodifiableMap((Map) this);
	}

	public String getUser() {
		return getProperty(USER);
	}

	public String getPassword() {
		return getProperty(PASSWORD);
	}

	public String getURL() {
		return getProperty(URL);
	}

	public String getEntityClasses() {
		return getProperty(ENTITY_CLASSES_PACKAGE);
	}

	/**
	 * Optional
	 * 
	 * @return true if key exists and value isn't empty
	 */
	public boolean isEntityClasses() {
		return checkContainsValue(ENTITY_CLASSES_PACKAGE);
	}

	private boolean checkContainsValue(String property) {
		if (!containsKey(property)) {
			return false;
		}
		String value = (String) get(property);
		return value != null && !value.isEmpty();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (Entry<Object, Object> property : entrySet()) {
			builder.append(',').append(property.getKey()).append('=').append(property.getValue());
		}
		return builder.toString();
	}
}