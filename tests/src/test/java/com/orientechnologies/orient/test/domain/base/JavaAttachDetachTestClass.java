/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.test.domain.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Id;
import javax.persistence.Transient;
import javax.persistence.Version;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.test.domain.business.Child;

/**
 * @author luca.molino
 * 
 */
public class JavaAttachDetachTestClass {
	public static final String		testStatic		= "10";
	@Transient
	public String									testTransient;
	@Id
	public Object									id;
	@Version
	public Object									version;
	public ODocument							embeddedDocument;
	public ODocument							document;
	public ORecordBytes						byteArray;
	public String									name;
	public Map<String, Child>			children			= new HashMap<String, Child>();
	public List<EnumTest>					enumList			= new ArrayList<EnumTest>();
	public Set<EnumTest>					enumSet				= new HashSet<EnumTest>();
	public Map<String, EnumTest>	enumMap				= new HashMap<String, EnumTest>();
	public String									text					= "initTest";
	public EnumTest								enumeration;
	public int										numberSimple	= 0;
	public long										longSimple		= 0l;
	public double									doubleSimple	= 0d;
	public float									floatSimple		= 0f;
	public byte										byteSimple		= 0;
	public boolean								flagSimple		= false;
}
