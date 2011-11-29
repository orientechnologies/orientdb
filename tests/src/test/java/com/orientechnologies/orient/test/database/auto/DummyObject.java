/**
 * 
 */
package com.orientechnologies.orient.test.database.auto;

import javax.persistence.Id;
import javax.persistence.Version;

/**
 * @author Michael Hiess
 * 
 */
public class DummyObject {

	@Id
	private Object	id;

	@Version
	private Object	version;

	private String	name;

	/**
	 * 
	 */
	public DummyObject() {
	}

	/**
	 * @param name
	 */
	public DummyObject(String name) {
		this.name = name;
	}

	/**
	 * @return the id
	 */
	public Object getId() {
		return this.id;
	}

	/**
	 * @param id
	 *          the id to set
	 */
	public void setId(Object id) {
		this.id = id;
	}

	/**
	 * @return the version
	 */
	public Object getVersion() {
		return this.version;
	}

	/**
	 * @param version
	 *          the version to set
	 */
	public void setVersion(Object version) {
		this.version = version;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @param name
	 *          the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

}
