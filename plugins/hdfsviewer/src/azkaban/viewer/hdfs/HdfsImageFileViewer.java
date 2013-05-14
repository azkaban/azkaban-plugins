package azkaban.viewer.hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

	/**
	* Reads a image file if the file size is not larger than {@value #MAX_IMAGE_FILE_SIZE}.
	* 
	* @author ximeng
	*
	*/

public class HdfsImageFileViewer implements HdfsFileViewer {

	private static Logger logger = Logger.getLogger(HdfsImageFileViewer.class);
	private static final long MAX_IMAGE_FILE_SIZE = 10485760L;

	private HashSet<String> acceptedSuffix;

	public HdfsImageFileViewer() {
		final String[] imageSuffix = {".jpg", ".jpeg", ".tif", ".tiff", ".png", ".gif", ".bmp", ".svg"};    
		acceptedSuffix = new HashSet<String>(Arrays.asList(imageSuffix));
	}

	public boolean canReadFile(FileSystem fs, Path path) {
		String fileName = path.getName();
		int pos = fileName.lastIndexOf('.');
		if(pos >= 0)
		{
			String suffix = fileName.substring(pos).toLowerCase();
			if(acceptedSuffix.contains(suffix))
			{
				long len = 0;
				try {
					len = fs.getFileStatus(path).getLen();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}
				if(len <= MAX_IMAGE_FILE_SIZE)
				{
					return true;
				}
			}
		}
		return false;
	}

	public void displayFile(FileSystem fs,
			Path path,
			OutputStream outputStream,
			int startLine,
			int endLine) throws IOException {

		if(logger.isDebugEnabled()) {
			logger.debug("read in image file");
		}

		InputStream inputStream = null;
		try {
			inputStream = new BufferedInputStream(fs.open(path));
			BufferedOutputStream output = new BufferedOutputStream(outputStream);
			long outputSize = 0L;
			byte[] buffer = new byte[16384];
			int len;
			while((len = inputStream.read(buffer)) != -1)
			{
				output.write(buffer, 0, len);
				outputSize += len;
				if (outputSize > MAX_IMAGE_FILE_SIZE)
					break;
			}
			output.flush();
		} finally {
			if (inputStream != null) {
				inputStream.close();
			}
		}
	}
}