/*
 * @author theleapofcode
 */
package com.orientechnologies.lucene.test;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

public class LuceneEmbeddedIndexingTest extends BaseLuceneTest {

	private void createMobileSchema() {
		db.command(new OCommandSQL("CREATE class mobile")).execute();
		db.command(new OCommandSQL("CREATE property mobile.mobileNumber STRING")).execute();

		db.command(new OCommandSQL("CREATE index mobile_mobileNumber on mobile (mobileNumber) UNIQUE")).execute();
	}

	private void createPersonSchema() {
		db.command(new OCommandSQL("CREATE class person")).execute();
		db.command(new OCommandSQL("CREATE property person.name STRING")).execute();
		db.command(new OCommandSQL("CREATE property person.mobile LINK mobile")).execute();
		db.command(new OCommandSQL("CREATE property person.mobileslist LINKLIST mobile")).execute();
		db.command(new OCommandSQL("CREATE property person.mobilesset LINKSET mobile")).execute();
		db.command(new OCommandSQL("CREATE property person.mobilesmap LINKMAP mobile")).execute();

		db.command(new OCommandSQL("CREATE index person_name on person (name) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL(
				"CREATE index person_mobile on person (`mobile.mobileNumber`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL(
				"CREATE index person_mobileslist on person (`mobileslist.mobileNumber`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL(
				"CREATE index person_mobilesset on person (`mobilesset.mobileNumber`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL(
				"CREATE index person_mobilesmap on person (`mobilesmap[personal].mobileNumber`) FULLTEXT ENGINE LUCENE STRING"))
				.execute(); // Indexed on value of particular key of the map
	}

	private void createCitizenSchema() {
		db.command(new OCommandSQL("CREATE class citizen")).execute();
		db.command(new OCommandSQL("CREATE property citizen.mobile LINK")).execute();

		db.command(new OCommandSQL("CREATE index citizen_mobile on citizen (mobile) FULLTEXT ENGINE LUCENE")).execute();
	}

	private void addMobilePersonCitizenData() {
		ODocument mobile1 = db.newInstance("mobile");
		mobile1.field("mobileNumber", "123");
		mobile1.save();

		ODocument mobile2 = db.newInstance("mobile");
		mobile2.field("mobileNumber", "321");
		mobile2.save();

		ODocument mobile3 = db.newInstance("mobile");
		mobile3.field("mobileNumber", "111");
		mobile3.save();

		ODocument mobile4 = db.newInstance("mobile");
		mobile4.field("mobileNumber", "222");
		mobile4.save();

		ODocument mobile5 = db.newInstance("mobile");
		mobile5.field("mobileNumber", "333");
		mobile5.save();

		ODocument mobile6 = db.newInstance("mobile");
		mobile6.field("mobileNumber", "444");
		mobile6.save();

		ODocument person1 = db.newInstance("person");
		person1.field("name", "person1");
		person1.field("mobile", mobile1);
		List<ODocument> elist1 = new LinkedList<ODocument>();
		elist1.add(mobile3);
		elist1.add(mobile4);
		person1.field("mobileslist", elist1);
		Set<ODocument> eset1 = new LinkedHashSet<ODocument>();
		eset1.add(mobile3);
		eset1.add(mobile4);
		person1.field("mobilesset", eset1);
		Map<String, ODocument> emap1 = new HashMap<String, ODocument>();
		emap1.put("official", mobile4);
		person1.field("mobilesmap", emap1);
		person1.save();

		emap1 = person1.field("mobilesmap");
		emap1.put("personal", mobile3);
		person1.save();

		ODocument person2 = db.newInstance("person");
		person2.field("name", "person2");
		person2.field("mobile", mobile2);
		List<ODocument> elist2 = new LinkedList<ODocument>();
		elist2.add(mobile5);
		elist2.add(mobile6);
		person2.field("mobileslist", elist2);
		Set<ODocument> eset2 = new LinkedHashSet<ODocument>();
		eset2.add(mobile5);
		eset2.add(mobile6);
		person2.field("mobilesset", eset2);
		Map<String, ODocument> emap2 = new HashMap<String, ODocument>();
		emap2.put("personal", mobile5);
		emap2.put("official", mobile6);
		person2.field("mobilesmap", emap2);
		person2.save();

		ODocument citizen1 = db.newInstance("citizen");
		citizen1.field("mobile", mobile1);
		citizen1.save();

		ODocument citizen2 = db.newInstance("citizen");
		citizen2.field("mobile", mobile2);
		citizen2.save();
	}

	private void createAddressSchema() {
		db.command(new OCommandSQL("CREATE class address")).execute();
		db.command(new OCommandSQL("CREATE property address.city STRING")).execute();
		db.command(new OCommandSQL("CREATE property address.pincode LONG")).execute();
		db.command(new OCommandSQL("CREATE index address_city on address (city) FULLTEXT ENGINE LUCENE")).execute();
	}

	private void createFooSchema() {
		db.command(new OCommandSQL("CREATE class foo")).execute();
		db.command(new OCommandSQL("CREATE property foo.name STRING")).execute();
		db.command(new OCommandSQL("CREATE property foo.bar INTEGER")).execute();
		db.command(new OCommandSQL("CREATE property foo.address EMBEDDED address")).execute();
		db.command(new OCommandSQL("CREATE property foo.xyz EMBEDDED")).execute();

		db.command(new OCommandSQL("CREATE index foo_name on foo (name) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index foo_bar on foo (bar) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index foo_address on foo (`address.city`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL("CREATE index foo_xyz on foo (`xyz.asd.zxc`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
	}

	private void addFooData() {
		db.command(new OCommandSQL("insert into foo (name, bar, address, xyz) values "
				+ "('abd', 1, {'pincode':123456, 'city':'NY', '@type':'d', '@class':'address'}, {'asd':{'zxc':'val1'}, '@type':'d'})"))
				.execute();
		db.command(new OCommandSQL("insert into foo (name, bar, address, xyz) values "
				+ "('asd', 2, {'pincode':654321, 'city':'LA', '@type':'d', '@class':'address'}, {'asd':{'zxc':'val2'}, '@type':'d'})"))
				.execute();
		db.command(new OCommandSQL("insert into foo content "
				+ "{ 'name': 'c', 'bar': 3, 'address': {'pincode':111111, 'city':'CHE', '@type':'d', '@class':'address'}, 'xyz': {'asd':{'zxc':'val3'}, '@type':'d'}}"))
				.execute();
	}

	private void createTestSchema() {
		db.command(new OCommandSQL("CREATE class test")).execute();
		db.command(new OCommandSQL("CREATE property test.name STRING")).execute();
		db.command(new OCommandSQL("CREATE property test.eliststr EMBEDDEDLIST STRING")).execute();
		db.command(new OCommandSQL("CREATE property test.esetstr EMBEDDEDSET STRING")).execute();
		db.command(new OCommandSQL("CREATE property test.emapstr EMBEDDEDMAP STRING")).execute();

		db.command(new OCommandSQL("CREATE index test_name on test (name) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index test_eliststr on test (eliststr) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index test_esetstr on test (esetstr) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index test_emapstr on test (emapstr by value) FULLTEXT ENGINE LUCENE"))
				.execute();
	}

	private void addTestData() {
		db.command(new OCommandSQL("insert into test (name, eliststr, esetstr, emapstr) values "
				+ "('test1', ['elistval1', 'elistval2', 'elistval3'], ['esetval1', 'esetval2', 'esetval3'], {'k1':'v1', 'k2':'v2'})"))
				.execute();
		db.command(new OCommandSQL("insert into test (name, eliststr, esetstr, emapstr) values "
				+ "('test2', ['elistval4', 'elistval5', 'elistval6'], ['esetval4', 'esetval5', 'esetval6'], {'k1':'v3', 'k2':'v4'})"))
				.execute();
		db.command(new OCommandSQL("insert into test (name, eliststr, esetstr, emapstr) values "
				+ "('test3', ['elistval7', 'elistval8', 'elistval9'], ['esetval7', 'esetval8', 'esetval9'], {'k1':'v5', 'k2':'v6'})"))
				.execute();
	}

	private void createDemoSchema() {
		db.command(new OCommandSQL("CREATE class demo")).execute();
		db.command(new OCommandSQL("CREATE property demo.name STRING")).execute();
		db.command(new OCommandSQL("CREATE property demo.elist EMBEDDEDLIST address")).execute();
		db.command(new OCommandSQL("CREATE property demo.eset EMBEDDEDSET address")).execute();
		db.command(new OCommandSQL("CREATE property demo.emap EMBEDDEDMAP address")).execute();

		db.command(new OCommandSQL("CREATE index demo_name on demo (name) FULLTEXT ENGINE LUCENE")).execute();
		db.command(new OCommandSQL("CREATE index demo_elist on demo (`elist.city`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL("CREATE index demo_eset on demo (`eset.city`) FULLTEXT ENGINE LUCENE STRING"))
				.execute();
		db.command(new OCommandSQL(
				"CREATE index demo_emap on demo (`emap[homeAddress].city`) FULLTEXT ENGINE LUCENE STRING")).execute();
	}

	private void addDemoData() {
		db.command(new OCommandSQL("insert into demo (name, elist, eset, emap) values "
				+ "('demo1', [{'@type':'d', '@class':'address','city':'NY','pincode':123456}, {'@type':'d', '@class':'address','city':'LA','pincode':654321}], "
				+ "[{'@type':'d', '@class':'address','city':'SJ','pincode':111111}, {'@type':'d', '@class':'address','city':'AU','pincode':222222}], "
				+ "{'homeAddress':{'@type':'d', '@class':'address','city':'NJ','pincode':333333}, 'officeAddress':{'@type':'d', '@class':'address','city':'WDC','pincode':444444}})"))
				.execute();
		db.command(new OCommandSQL("insert into demo (name, elist, eset, emap) values "
				+ "('demo2', [{'@type':'d', '@class':'address','city':'BLR','pincode':555555}, {'@type':'d', '@class':'address','city':'MYS','pincode':666666}], "
				+ "[{'@type':'d', '@class':'address','city':'ND','pincode':777777}, {'@type':'d', '@class':'address','city':'MUM','pincode':888888}], "
				+ "{'homeAddress':{'@type':'d', '@class':'address','city':'HYD','pincode':999999}, 'officeAddress':{'@type':'d', '@class':'address','city':'CH','pincode':876789}})"))
				.execute();
	}

	@Before
	public void init() throws Exception {
		// For Link/LinkList/LinkSet/LinkMap
		createMobileSchema();
		createPersonSchema();
		createCitizenSchema();
		addMobilePersonCitizenData();

		// For Primitives/Embedded
		createAddressSchema();
		createFooSchema();
		addFooData();

		// For Primitives/EmbeddedList/Set/Map of primitives
		createTestSchema();
		addTestData();

		// For EmbeddedList/Set/Map of LinkedClass
		createDemoSchema();
		addDemoData();
	}

	@Test
	public void testPrimitive() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where name lucene 'asd'")).execute();
		Assert.assertEquals(1, qResult.size());

		List<ODocument> qResult2 = db.command(new OCommandSQL("select * from foo where name lucene 'a*d'")).execute();
		Assert.assertEquals(2, qResult2.size());
	}

	@Test
	public void testLink() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from person where mobile.mobileNumber lucene '123'")).execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testLinkList() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from person where mobileslist.mobileNumber lucene '111'")).execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testLinkSet() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from person where mobilesset.mobileNumber lucene '111'")).execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testLinkMap() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from person where mobilesmap[personal].mobileNumber lucene '111'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedWithLinkedClass() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where address.city lucene 'NY'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedWithLinkedClassOnUpdate() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where address.city lucene 'CHE'"))
				.execute();
		Assert.assertEquals(qResult.size(), 1);

		db.command(new OCommandSQL(
				"update foo set address= {'pincode':222222, 'city':'BLR', '@type':'d', '@class':'address'} where name='c'"))
				.execute();

		List<ODocument> qResult2 = db.command(new OCommandSQL("select * from foo where address.city lucene 'CHE'"))
				.execute();
		Assert.assertEquals(qResult2.size(), 0);

		List<ODocument> qResult3 = db.command(new OCommandSQL("select * from foo where address.city lucene 'BLR'"))
				.execute();
		Assert.assertEquals(qResult3.size(), 1);
	}

	@Test
	public void testEmbeddedWithoutLinkedClass() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from foo where xyz.asd.zxc lucene 'val2'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedListOfPrimitives() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from test where eliststr lucene 'elistval2'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedSetOfPrimitives() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from test where esetstr lucene 'esetval4'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedMapOfPrimitives() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from test where emapstr containsvalue 'v1' and emapstr lucene 'v1'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedListOfLinkedClass() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from demo where elist.city lucene 'BLR'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedSetOfLinkedClass() throws Exception {
		List<ODocument> qResult = db.command(new OCommandSQL("select * from demo where eset.city lucene 'SJ'"))
				.execute();
		Assert.assertEquals(1, qResult.size());
	}

	@Test
	public void testEmbeddedMapOfLinkedClass() throws Exception {
		List<ODocument> qResult = db
				.command(new OCommandSQL("select * from demo where emap[homeAddress].city lucene 'N*'")).execute();
		Assert.assertEquals(1, qResult.size());
	}

}
