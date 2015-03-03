package com.orientechnologies.website;

public class OLicenseManagerTest {

//  @Test(expected = OLicenseException.class)
//  public void testExpiresToday() throws OLicenseException {
//    final Calendar now = new GregorianCalendar();
//    final String license = OLicenseManagerConsole.generate(100, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//        now.get(Calendar.DAY_OF_MONTH));
//    OL.checkDate(license);
//  }
//
//  @Test(expected = OLicenseException.class)
//  public void testExpiresYesterday() throws OLicenseException {
//    final Calendar now = new GregorianCalendar();
//    now.add(Calendar.DAY_OF_MONTH, -1);
//    final String license = OLicenseManagerConsole.generate(100, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//        now.get(Calendar.DAY_OF_MONTH));
//    OL.checkDate(license);
//  }
//
//  @Test
//  public void testExpiresTomorrow() throws OLicenseException {
//    final Calendar now = new GregorianCalendar();
//    now.add(Calendar.DAY_OF_MONTH, +1);
//    final String license = OLicenseManagerConsole.generate(100, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//        now.get(Calendar.DAY_OF_MONTH));
//    Assert.assertEquals(OL.checkDate(license), 0);
//  }
//
//  @Test
//  public void testClientId() {
//    final Calendar now = new GregorianCalendar();
//    now.add(Calendar.DAY_OF_MONTH, +1);
//
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(100, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 100);
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(0, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 0);
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(123456, 23020, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 123456);
//  }
//
//  @Test
//  public void testServerId() {
//    final Calendar now = new GregorianCalendar();
//    now.add(Calendar.DAY_OF_MONTH, +1);
//
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(0, 0, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 0);
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(100, 100, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 100);
//    Assert.assertEquals(
//        OL.getClientId(OLicenseManagerConsole.generate(123456, 123456, now.get(Calendar.YEAR), now.get(Calendar.MONTH) + 1,
//            now.get(Calendar.DAY_OF_MONTH))), 123456);
//  }
}