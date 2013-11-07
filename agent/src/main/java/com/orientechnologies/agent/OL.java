/*
 * Copyright 2010-2013 Orient Technologies LTD (info--at--orientechnologies.com)
 * All Rights Reserved. Commercial License.
 * 
 * NOTICE:  All information contained herein is, and remains the property of
 * Orient Technologies LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * Orient Technologies LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 * 
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Orient Technologies LTD.
 * 
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.agent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("restriction")
public class OL {

  public static int checkDate(final String iLicense) throws OLicenseException {
    if (iLicense == null || iLicense.isEmpty())
      throw new OLicenseException("license not found");

    final String key = "@Ld" + "ks#2" + "" + "3dsLvc" + (35 - 12 * 2) + "a!Po" + "weRr";
    try {
      final Date now = new Date();
      final Date d = new SimpleDateFormat("yyyyMMdd").parse(OCry.decrypt(iLicense, key).substring(12));
      if (!d.after(now))
        throw new OLicenseException("license expired on: " + d);
      return getDateDiff(d, now);
    } catch (Exception e) {
      throw new OLicenseException("license not valid");
    }
  }

  public static int getClientId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(27 - 4) + "dsLvc" + (13 - 4 + 2) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(OCry.decrypt(iLicense, key).substring(0, 6));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  public static int getServerId(final String iLicense) {
    final String key = "@Ld" + "ks#" + new Integer(23 + 9 - 19 + 10) + "dsLvc" + (110 / 10) + "a!Po" + "weRr";
    try {
      return Integer.parseInt(OCry.decrypt(iLicense, key).substring(6, 12));
    } catch (Exception e) {
      throw new RuntimeException("License not valid");
    }
  }

  private static int getDateDiff(Date date1, Date date2) {
    final long diffInMillies = date2.getTime() - date1.getTime();
    return (int) TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
  }
}
