package com.orientechnologies.orient.object.enumerations;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

/**
 * @author JN <a href="mailto:jn@brain-activit.com">Julian Neuhaus</a>
 * @since 15.08.2014
 */
public class OObjectEnumLazyListTest
{
	public enum TESTENUM
	{
		TEST_VALUE_1,
		TEST_VALUE_2,
		TEST_VALUE_3
	}
	
	public enum WRONG_TESTENUM
	{
		TEST_VALUE_1,
		TEST_VALUE_2,
		TEST_VALUE_3
	}
	
	private OObjectDatabaseTx databaseTx;
	
	@BeforeClass
	protected void setUp() throws Exception {
		databaseTx = new OObjectDatabaseTx("memory:OObjectEnumLazyListTest");
		databaseTx.create();

		databaseTx.getEntityManager().registerEntityClass(EntityWithEnumList.class);

	}

	@AfterClass
	protected void tearDown() {
				
		databaseTx.drop();
	}

	@Test
	public void containsTest() {
		
		EntityWithEnumList hasListWithEnums = getTestObject();	
		
		assertTrue(hasListWithEnums.getEnumList().contains(TESTENUM.TEST_VALUE_1));
		assertTrue(hasListWithEnums.getEnumList().contains(TESTENUM.TEST_VALUE_2));
		assertTrue(hasListWithEnums.getEnumList().contains(TESTENUM.TEST_VALUE_3));
		
		assertFalse(hasListWithEnums.getEnumList().contains(WRONG_TESTENUM.TEST_VALUE_1));
		assertFalse(hasListWithEnums.getEnumList().contains(WRONG_TESTENUM.TEST_VALUE_2));
		assertFalse(hasListWithEnums.getEnumList().contains(WRONG_TESTENUM.TEST_VALUE_3));
		
		assertFalse(hasListWithEnums.getEnumList().contains(null));		
		assertFalse(hasListWithEnums.getEnumList().contains("INVALID TYPE"));
	}	
	
	@Test
	public void indexOfTest() {
		
		EntityWithEnumList hasListWithEnums = getTestObject();	
		
		assertTrue(hasListWithEnums.getEnumList().indexOf(TESTENUM.TEST_VALUE_1) == 0);
		assertTrue(hasListWithEnums.getEnumList().indexOf(TESTENUM.TEST_VALUE_2) == 1);
		assertTrue(hasListWithEnums.getEnumList().indexOf(TESTENUM.TEST_VALUE_3) == 2);
		
		assertTrue(hasListWithEnums.getEnumList().indexOf(WRONG_TESTENUM.TEST_VALUE_1) == -1);
		assertTrue(hasListWithEnums.getEnumList().indexOf(WRONG_TESTENUM.TEST_VALUE_2) == -1);
		assertTrue(hasListWithEnums.getEnumList().indexOf(WRONG_TESTENUM.TEST_VALUE_3) == -1);
		
		assertTrue(hasListWithEnums.getEnumList().indexOf(null) == -1);
		assertTrue(hasListWithEnums.getEnumList().indexOf("INVALID TYPE") == -1);
	}	
	
	@Test
	public void lastIndexOfTest() {
		
		EntityWithEnumList hasListWithEnums = getTestObject();	
		
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(TESTENUM.TEST_VALUE_1) == 3);
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(TESTENUM.TEST_VALUE_2) == 4);
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(TESTENUM.TEST_VALUE_3) == 5);
		
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(WRONG_TESTENUM.TEST_VALUE_1) == -1);
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(WRONG_TESTENUM.TEST_VALUE_2) == -1);
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(WRONG_TESTENUM.TEST_VALUE_3) == -1);
		
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf(null) == -1);
		assertTrue(hasListWithEnums.getEnumList().lastIndexOf("INVALID TYPE") == -1);
	}	
	
	private EntityWithEnumList getTestObject()
	{
		EntityWithEnumList toSave = new EntityWithEnumList();
		
		List<TESTENUM> enumList = new ArrayList<TESTENUM>();
		
		enumList.add(TESTENUM.TEST_VALUE_1);
		enumList.add(TESTENUM.TEST_VALUE_2);
		enumList.add(TESTENUM.TEST_VALUE_3);
		enumList.add(TESTENUM.TEST_VALUE_1);
		enumList.add(TESTENUM.TEST_VALUE_2);
		enumList.add(TESTENUM.TEST_VALUE_3);
		
		toSave.setEnumList(enumList);
		
		EntityWithEnumList proxiedEntitiy = databaseTx.save(toSave);
		
		return proxiedEntitiy;
	}
	
	public class EntityWithEnumList
	{
		private List<TESTENUM> enumList;

		public EntityWithEnumList()
		{
			super();
		}
		
		public List<TESTENUM> getEnumList()
		{
			return enumList;
		}

		public void setEnumList(List<TESTENUM> enumList)
		{
			this.enumList = enumList;
		}		
	}
}
