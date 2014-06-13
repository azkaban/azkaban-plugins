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

package azkaban.reportal.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import azkaban.security.commons.HadoopSecurityManager;
import azkaban.security.commons.HadoopSecurityManagerException;

public class StreamProviderHDFS implements IStreamProvider {

  FileSystem hdfs;
  HadoopSecurityManager securityManager;
  String username;

  public void setHadoopSecurityManager(HadoopSecurityManager securityManager) {
    this.securityManager = securityManager;
  }

  public void setUser(String user) {
    this.username = user;
  }

  public String[] getFileList(String pathString)
      throws HadoopSecurityManagerException, IOException {
    FileStatus[] statusList = getFileStatusList(pathString);
    String[] fileList = new String[statusList.length];

    for (int i = 0; i < statusList.length; i++) {
      fileList[i] = statusList[i].getPath().getName();
    }

    return fileList;
  }

  public String[] getOldFiles(String pathString, long thresholdTime)
      throws Exception {
    FileStatus[] statusList = getFileStatusList(pathString);

    List<String> oldFiles = new ArrayList<String>();

    for (FileStatus fs : statusList) {
      if (fs.getModificationTime() < thresholdTime) {
        oldFiles.add(fs.getPath().getName());
      }
    }

    return oldFiles.toArray(new String[0]);
  }

  public void deleteFile(String pathString) throws Exception {
    ensureHdfs();

    try {
      hdfs.delete(new Path(pathString), true);
    } catch (IOException e) {
      cleanUp();
    }
  }

  public InputStream getFileInputStream(String pathString) throws Exception {
    ensureHdfs();

    Path path = new Path(pathString);

    return new BufferedInputStream(hdfs.open(path));
  }

  public OutputStream getFileOutputStream(String pathString) throws Exception {
    ensureHdfs();

    Path path = new Path(pathString);

    return new BufferedOutputStream(hdfs.create(path, true));
  }

  public void cleanUp() throws IOException {
    if (hdfs != null) {
      hdfs.close();
      hdfs = null;
    }
  }

  private void ensureHdfs() throws HadoopSecurityManagerException, IOException {
    if (hdfs == null) {
      if (securityManager == null) {
        hdfs = FileSystem.get(new Configuration());
      } else {
        hdfs = securityManager.getFSAsUser(username);
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    cleanUp();
    super.finalize();
  }

  /**
   * Returns an array of the file statuses of the files/directories in the given
   * path if it is a directory and an empty array otherwise.
   *
   * @param pathString
   * @return
   * @throws HadoopSecurityManagerException
   * @throws IOException
   */
  private FileStatus[] getFileStatusList(String pathString)
      throws HadoopSecurityManagerException, IOException {
    ensureHdfs();

    Path path = new Path(pathString);
    FileStatus pathStatus = null;
    try {
      pathStatus = hdfs.getFileStatus(path);
    } catch (IOException e) {
      cleanUp();
    }

    if (pathStatus != null && pathStatus.isDir()) {
      return hdfs.listStatus(path);
    }

    return new FileStatus[0];
  }
}
