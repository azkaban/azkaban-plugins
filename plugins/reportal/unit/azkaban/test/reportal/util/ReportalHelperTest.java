/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.test.reportal.util;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import azkaban.reportal.util.Reportal;
import azkaban.reportal.util.ReportalHelper;

public class ReportalHelperTest {

  @Test
  public void testParseUniqueEmailsNullString() {
    Set<String> emails =
        ReportalHelper
            .parseUniqueEmails(null, Reportal.ACCESS_LIST_SPLIT_REGEX);
    Assert.assertTrue(emails.size() == 0);
  }

  @Test
  public void testParseUniqueEmailsEmptyString() {
    Set<String> emails =
        ReportalHelper.parseUniqueEmails("", Reportal.ACCESS_LIST_SPLIT_REGEX);
    Assert.assertTrue(emails.size() == 0);
  }

  @Test
  public void testParseUniqueEmailsCommas() {
    Set<String> emails =
        ReportalHelper.parseUniqueEmails(
            "test@example.com, test2@example2.com ,test3@example3.com",
            Reportal.ACCESS_LIST_SPLIT_REGEX);

    Set<String> expectedEmails = new HashSet<String>();
    expectedEmails.add("test@example.com");
    expectedEmails.add("test2@example2.com");
    expectedEmails.add("test3@example3.com");

    Assert.assertTrue(emails.size() == 3);
    Assert.assertTrue(emails.containsAll(expectedEmails));
  }

  @Test
  public void testParseUniqueEmailsSemicolons() {
    Set<String> emails =
        ReportalHelper.parseUniqueEmails(
            "test@example.com; test2@example2.com ;test3@example3.com",
            Reportal.ACCESS_LIST_SPLIT_REGEX);

    Set<String> expectedEmails = new HashSet<String>();
    expectedEmails.add("test@example.com");
    expectedEmails.add("test2@example2.com");
    expectedEmails.add("test3@example3.com");

    Assert.assertTrue(emails.size() == 3);
    Assert.assertTrue(emails.containsAll(expectedEmails));
  }

  @Test
  public void testParseUniqueEmailsWhitespace() {
    Set<String> emails =
        ReportalHelper.parseUniqueEmails(
            "test@example.com test2@example2.com\ntest3@example3.com",
            Reportal.ACCESS_LIST_SPLIT_REGEX);

    Set<String> expectedEmails = new HashSet<String>();
    expectedEmails.add("test@example.com");
    expectedEmails.add("test2@example2.com");
    expectedEmails.add("test3@example3.com");

    Assert.assertTrue(emails.size() == 3);
    Assert.assertTrue(emails.containsAll(expectedEmails));
  }

  @Test
  public void testIsValidEmailAddressNullString() {
    String email = null;
    Assert.assertFalse(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testIsValidEmailAddressEmptyString() {
    String email = "";
    Assert.assertFalse(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testIsValidEmailAddressValidEmail() {
    String email = "test@example.com";
    Assert.assertTrue(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testIsValidEmailAddressMissingDomain() {
    String email = "test@";
    Assert.assertFalse(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testIsValidEmailAddressMissingLocalPart() {
    String email = "@example.com";
    Assert.assertFalse(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testIsValidEmailAddressMultipleDomains() {
    String email = "test@example.com@example2.com";
    Assert.assertFalse(ReportalHelper.isValidEmailAddress(email));
  }

  @Test
  public void testGetEmailDomainNullString() {
    String email = null;
    Assert.assertNull(ReportalHelper.getEmailDomain(email));
  }

  @Test
  public void testGetEmailDomainEmptyString() {
    String email = "";
    Assert.assertNull(ReportalHelper.getEmailDomain(email));
  }

  @Test
  public void testGetEmailDomainValidEmail() {
    String email = "test@example.com";
    Assert.assertEquals("example.com", ReportalHelper.getEmailDomain(email));
  }

}
