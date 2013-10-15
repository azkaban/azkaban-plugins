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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamProviderLocal implements IStreamProvider {

	public void setUser(String user) {
	}
	
	public String[] getFileList(String pathString) throws IOException {
		File file = new File(pathString);

		if (file.exists() && file.isDirectory()) {
			return file.list();
		}

		return new String[0];
	}

	public InputStream getFileInputStream(String pathString) throws IOException {

		File inputFile = new File(pathString);

		inputFile.getParentFile().mkdirs();
		inputFile.createNewFile();

		return new BufferedInputStream(new FileInputStream(inputFile));
	}

	public OutputStream getFileOutputStream(String pathString) throws IOException {

		File outputFile = new File(pathString);

		outputFile.getParentFile().mkdirs();
		outputFile.createNewFile();

		return new BufferedOutputStream(new FileOutputStream(outputFile));
	}

	public void cleanUp() throws IOException {
	}
}
