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
