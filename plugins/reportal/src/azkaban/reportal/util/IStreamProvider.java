package azkaban.reportal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface IStreamProvider {

	public void setUser(String user);
	
	public String[] getFileList(String pathString) throws Exception;

	public InputStream getFileInputStream(String pathString) throws Exception;

	public OutputStream getFileOutputStream(String pathString) throws Exception;
	
	public void cleanUp() throws IOException;
}
