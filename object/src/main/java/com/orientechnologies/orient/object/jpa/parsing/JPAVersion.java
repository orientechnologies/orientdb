package com.orientechnologies.orient.object.jpa.parsing;

public enum JPAVersion {

	V1_0((byte) 1, (byte) 0), V2_0((byte) 2, (byte) 0), V2_1((byte) 2, (byte) 1);

	public final byte	major;
	public final byte	minor;

	JPAVersion(final byte major, final byte minor) {
		this.major = major;
		this.minor = minor;
	}

	/**
	 * @return jpa version formated as MAJOR_MINOR
	 */
	public String getVersion() {
		return String.format("%d.%d", major, minor);
	}

	public String getFilename() {
		return String.format("persistence_%d_%d.xsd", major, minor);
	}
	
	public static JPAVersion parse(String version) {	
		return valueOf("V"+version.charAt(0)+'_'+ version.charAt(2));
	}
	
	@Override
	public String toString() {
		return String.format("%d_%d", major, minor);
	}
}
