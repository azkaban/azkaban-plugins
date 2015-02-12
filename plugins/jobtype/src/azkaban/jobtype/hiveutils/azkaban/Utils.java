/*
 * Copyright 2012 LinkedIn Corp.
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

package azkaban.jobtype.hiveutils.azkaban;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class Utils {

  private final static Logger LOG = Logger.getLogger("com.linkedin.hive.azkaban.hive.Utils");

  public static class QueryPropKeys {
    final String singleQuery;
    final String multilineQuery;
    final String fromFile;
    final String fromUrl;

    public QueryPropKeys(String singleQuery, String multilineQuery, String fromFile, String fromUrl) {
      this.singleQuery = singleQuery;
      this.multilineQuery = multilineQuery;
      this.fromFile = fromFile;
      this.fromUrl = fromUrl;
    }
  }

  public static String joinNewlines(Collection<String> strings) {
    if (strings == null || strings.size() == 0)
      return null;

    StringBuilder sb = new StringBuilder();

    for (String s : strings) {
      String trimmed = s.trim();
      sb.append(trimmed);
      if (!trimmed.endsWith("\n"))
        sb.append("\n");
    }

    return sb.toString();
  }

  // Hey, look! It's this method again! It's the freaking Where's Waldo of
  // methods...
  public static String verifyProperty(Properties p, String key)
      throws HiveViaAzkabanException {
    String value = p.getProperty(key);
    if (value == null) {
      throw new HiveViaAzkabanException("Can't find property " + key
          + " in provided Properties. Bailing");
    }
    // TODO: Add a log entry here for the value
    return value;

  }

  public static String determineQuery(Properties properties, QueryPropKeys keys) throws HiveViaAzkabanException {
    String singleLine = properties.getProperty(keys.singleQuery);
    String multiLine = extractMultilineQuery(properties, keys.multilineQuery);
    String queryFile = extractQueryFromFile(properties, keys.fromFile);
    String queryURL = extractQueryFromURL(properties, keys.fromUrl);

    return determineQuery(singleLine, multiLine, queryFile, queryURL, keys);
  }

  private static String extractQueryFromFile(Properties properties, String HIVE_QUERY_FILE) throws HiveViaAzkabanException {
    String file = properties.getProperty(HIVE_QUERY_FILE);

    if(file == null) return null;

    LOG.info("Attempting to read query from file: " + file);

    StringBuilder contents = new StringBuilder();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(file));

      String line;

      while((line = br.readLine()) != null) {
        contents.append(line);
        contents.append(System.getProperty("line.separator"));
      }

    } catch (IOException e) {
      throw new HiveViaAzkabanException(e);
    } finally {
      if(br != null) try {
        br.close();
      } catch (IOException e) {
        // TODO: Just throw IOException and catch-wrap in the constructor...
        throw new HiveViaAzkabanException(e);
      }
    }

    return contents.toString();
  }

  private static String extractQueryFromURL(Properties properties, String queryUrlKey) throws HiveViaAzkabanException{
    String url = properties.getProperty(queryUrlKey);

    if(url == null) return null;

    LOG.info("Attempting to retrieve query from URL: " + url);


    StringBuilder contents = new StringBuilder();
    BufferedReader br = null;

    try {
      URL queryURL = new URL(url);

      br = new BufferedReader( new InputStreamReader(queryURL.openStream()));
      String line;

      while ((line = br.readLine()) != null){
        contents.append(line);
        contents.append(System.getProperty("line.separator"));
      }
    } catch (IOException e) {
      throw new HiveViaAzkabanException(e);
    } finally {
      if(br != null) try {
        br.close();
      } catch (IOException e) {
        // TODO: Just throw IOException and catch-wrap in the constructor...
        throw new HiveViaAzkabanException(e);
      }
    }

    return contents.toString();
  }


  private static String determineQuery(String singleLine, String multiLine, String queryFromFile, String queryFromURL, QueryPropKeys keys)
          throws HiveViaAzkabanException {
    int specifiedValues = 0;

    for(String s : new String [] { singleLine, multiLine, queryFromFile, queryFromURL}) {
      if(s != null) specifiedValues++;
    }

    if(specifiedValues == 0)
      throw new HiveViaAzkabanException("Must specify " + keys.singleQuery + " xor " + keys.multilineQuery + ".nn xor " + keys.fromFile + " xor " + keys.fromUrl + " in properties. Exiting.");

    if(specifiedValues != 1)
      throw new HiveViaAzkabanException("Must specify only " + keys.singleQuery + " or " + keys.multilineQuery + ".nn or " + keys.fromFile + " or " + keys.fromUrl + " in properties, not more than one. Exiting.");

    if(singleLine != null) {
      LOG.info("Returning " + keys.singleQuery + " = " + singleLine);
      return singleLine;
    } else if(multiLine != null) {
      LOG.info("Returning consolidated " + keys.multilineQuery + ".nn = " + multiLine);
      return multiLine;
    } else if(queryFromFile != null){
      LOG.info("Returning query from file " + queryFromFile);
      return queryFromFile;
    } else {
      LOG.info("Returning query from URL " + queryFromURL);
      return queryFromURL;
    }
  }

  private static String extractMultilineQuery(Properties properties, String HIVE_QUERY) {
    ArrayList<String> lines = new ArrayList<String>();

    for(int i = 0; i < 100; i++) {
      String padded = String.format("%02d", i);
      String value = properties.getProperty(HIVE_QUERY + "." + padded);
      if(value != null) {
        lines.add(value);
      }
    }

    return Utils.joinNewlines(lines);
  }
}
