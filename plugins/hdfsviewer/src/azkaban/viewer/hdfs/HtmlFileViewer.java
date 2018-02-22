/*
 * Copyright 2018 LinkedIn Corp.
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

package azkaban.viewer.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AccessControlException;
import org.apache.log4j.Logger;

public class HtmlFileViewer extends HdfsFileViewer {

  private static Logger logger = Logger.getLogger(HtmlFileViewer.class);
  private Set<String> acceptedSuffix = new HashSet<>();

  // only display the first 25M chars. it is used to prevent
  // showing/downloading gb of data
  private static final int BUFFER_LIMIT = 25000000;
  private static final String VIEWER_NAME = "Html";

  public HtmlFileViewer() {
    acceptedSuffix.add(".htm");
    acceptedSuffix.add(".html");
  }

  @Override
  public String getName() {
    return VIEWER_NAME;
  }


  @Override
  public Set<Capability> getCapabilities(FileSystem fs, Path path)
      throws AccessControlException {
    String fileName = path.getName();
    int pos = fileName.lastIndexOf('.');
    if (pos < 0) {
      return EnumSet.noneOf(Capability.class);
    }

    String suffix = fileName.substring(pos).toLowerCase();
    if (acceptedSuffix.contains(suffix)) {
      return EnumSet.of(Capability.READ);
    } else {
      return EnumSet.noneOf(Capability.class);
    }
  }

  public void displayFile(FileSystem fs, Path path, OutputStream outputStream,
      int startLine, int endLine) throws IOException {

    if (logger.isDebugEnabled())
      logger.debug("read in uncompressed html file");

    TextFileViewer.displayFileContent(fs, path, outputStream, startLine, endLine, BUFFER_LIMIT);
  }

  public ContentType getContentType() {
    return ContentType.HTML;
  }
}
