/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.domain.whiz;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.annotation.OId;
import com.orientechnologies.orient.core.annotation.OVersion;
import com.orientechnologies.orient.test.domain.business.Address;

public class Profile {
	@OId
	private String				id;
	@OVersion
	private Integer				version;
	private String				nick;
	private Set<Profile>	followings	= new HashSet<Profile>();
	private Set<Profile>	followers		= new HashSet<Profile>();
	private String				name;
	private String				surname;
	private Address				location;
	private Long					hash;
	private Profile				invitedBy;
	private String				value;

	public Profile() {
	}

	public Profile(String iNick) {
		nick = iNick;
	}

	public Profile(String iNick, String iName, String iSurname, Profile iInvitedBy) {
		this.nick = iNick;
		this.name = iName;
		this.surname = iSurname;
		this.invitedBy = iInvitedBy;
	}

	public Set<Profile> getFollowings() {
		return followings;
	}

	public Set<Profile> getFollowers() {
		return followers;
	}

	public Profile addFollower(Profile iFollower) {
		followers.add(iFollower);
		iFollower.followings.add(this);
		return this;
	}

	public Profile removeFollower(Profile iFollower) {
		followers.remove(iFollower);
		iFollower.followings.remove(this);
		return this;
	}

	public Profile addFollowing(Profile iFollowing) {
		followings.add(iFollowing);
		iFollowing.followers.add(this);
		return this;
	}

	public Profile removeFollowing(Profile iFollowing) {
		followings.remove(iFollowing);
		iFollowing.followers.remove(this);
		return this;
	}

	public Profile getInvitedBy() {
		return invitedBy;
	}

	public String getName() {
		return name;
	}

	public Profile setName(String name) {
		this.name = name;
		return this;
	}

	public String getSurname() {
		return surname;
	}

	public Profile setSurname(String surname) {
		this.surname = surname;
		return this;
	}

	public Address getLocation() {
		return location;
	}

	public Profile setLocation(Address location) {
		this.location = location;
		return this;
	}

	public Profile setInvitedBy(Profile invitedBy) {
		this.invitedBy = invitedBy;
		return this;
	}

	public String getNick() {
		return nick;
	}

	public void setNick(String nick) {
		this.nick = nick;
	}

	public String getValue() {
		return value;
	}

	public Profile setValue(String value) {
		this.value = value;
		return this;
	}

	public String getId() {
		return id;
	}

	public Long getHash() {
		return hash;
	}

	public Profile setHash(Long hash) {
		this.hash = hash;
		return this;
	}
}
