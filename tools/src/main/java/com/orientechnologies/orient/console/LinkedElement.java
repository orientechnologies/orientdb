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
package com.orientechnologies.orient.console;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.annotation.OAfterDeserialization;
import com.orientechnologies.orient.core.annotation.OAfterSerialization;

/**
 * @author luca.molino
 * 
 */
public class LinkedElement<T> {

	public class LinkedElementMap {

		protected String	value;

		public LinkedElementMap() {
		}

		public LinkedElementMap(String iValue) {
			value = iValue;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	protected T															value;

	protected int														valore;

	protected Date													data;

	protected double												decimale;

	protected LinkedElement<T>							next;

	protected List<Integer>									listaInt;

	protected List<String>									listaString;

	protected List<LinkedElementMap>				lista;

	protected Set<LinkedElementMap>					set;

	protected Map<String, LinkedElementMap>	mappa;

	// protected LinkedElementMap[] array;

	public LinkedElement() {
		this(null, null);
		lista = new ArrayList<LinkedElementMap>();
		listaInt = new ArrayList<Integer>();
		listaString = new ArrayList<String>();
		set = new HashSet<LinkedElementMap>();
		mappa = new HashMap<String, LinkedElementMap>();
	}

	public LinkedElement(T value, LinkedElement<T> next) {
		this.value = value;
		this.next = next;
	}

	public LinkedElement(T value, LinkedElement<T> next, int valore) {
		this.value = value;
		this.next = next;
		this.valore = valore;
	}

	public LinkedElement<T> getNext() {
		return next;
	}

	public void setNext(LinkedElement<T> next) {
		this.next = next;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	public int getValore() {
		return valore;
	}

	public void setValore(int valore) {
		this.valore = valore;
	}

	public Date getData() {
		return data;
	}

	public void setData(Date data) {
		this.data = data;
	}

	public double getDecimale() {
		return decimale;
	}

	public void setDecimale(double decimale) {
		this.decimale = decimale;
	}

	//
	// public LinkedElementMap[] getArray() {
	// return array;
	// }
	//
	// public void setArray(LinkedElementMap[] array) {
	// this.array = array;
	// }

	public List<LinkedElementMap> getLista() {
		return lista;
	}

	public void setLista(List<LinkedElementMap> lista) {
		this.lista = lista;
	}

	public List<Integer> getListaInt() {
		return listaInt;
	}

	public void setListaInt(List<Integer> listaInt) {
		this.listaInt = listaInt;
	}

	public List<String> getListaString() {
		return listaString;
	}

	public void setListaString(List<String> listaString) {
		this.listaString = listaString;
	}

	public Set<LinkedElementMap> getSet() {
		return set;
	}

	public void setSet(Set<LinkedElementMap> set) {
		this.set = set;
	}

	public Map<String, LinkedElementMap> getMappa() {
		return mappa;
	}

	public void setMappa(Map<String, LinkedElementMap> mappa) {
		this.mappa = mappa;
	}

	@OAfterDeserialization
	public void afterLoad() {
		System.out.printf("Loaded - val: %s; next: %s%n", value, next != null);
	}

	@OAfterSerialization
	public void afterSave() {
		System.out.printf("Saved - id: %s; ver: %s; val: %s; next: %s%n", value, next != null);
	}
}
