package azkaban.reportal.util;

public class ReportalUtil {
	public static IStreamProvider getStreamProvider(String fileSystem) {
		if (fileSystem.equalsIgnoreCase("hdfs")) {
			return new StreamProviderHDFS();
		}
		return new StreamProviderLocal();
	}
}
