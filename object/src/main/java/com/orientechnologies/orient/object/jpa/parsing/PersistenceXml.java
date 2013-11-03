package com.orientechnologies.orient.object.jpa.parsing;


public enum PersistenceXml {

	TAG_PERSISTENCE("persistence"), TAG_PERSISTENCE_UNIT("persistence-unit"), TAG_PROPERTIES("properties"), TAG_PROPERTY("property"), TAG_NON_JTA_DATA_SOURCE(
			"non-jta-data-source"), TAG_JTA_DATA_SOURCE("jta-data-source"), TAG_CLASS("class"), TAG_MAPPING_FILE("mapping-file"), TAG_JAR_FILE(
			"jar-file"), TAG_EXCLUDE_UNLISTED_CLASSES("exclude-unlisted-classes"), TAG_VALIDATION_MODE("validation-mode"), TAG_SHARED_CACHE_MODE(
			"shared-cache-mode"), TAG_PROVIDER("provider"), TAG_UNKNOWN$("unknown$"), ATTR_UNIT_NAME("name"), ATTR_TRANSACTION_TYPE(
			"transaction-type"), ATTR_SCHEMA_VERSION("version");

	private final String	name;

	PersistenceXml(String name) {
		this.name = name;
	}

	/**
	 * Case ignorance, null safe method
	 * 
	 * @param aName
	 * @return true if tag equals to enum item
	 */
	public boolean equals(String aName) {
		return name.equalsIgnoreCase(aName);
	}

	/**
	 * Try to parse tag to enum item
	 * 
	 * @param aName
	 * @return TAG_UNKNOWN$ if failed to parse
	 */
	public static PersistenceXml parse(String aName) {
		try {
			return valueOf("TAG_" + aName.replace('-', '_').toUpperCase());
		} catch (IllegalArgumentException e) {
			return TAG_UNKNOWN$;
		}

	}

	@Override
	public String toString() {
		return name;
	}
}
