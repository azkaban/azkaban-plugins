package azkaban.security;

public class HadoopSecurityManagerException extends Exception {
	private static final long serialVersionUID = 1L;
	public HadoopSecurityManagerException(String message) {
		super(message);
	}
	
	public HadoopSecurityManagerException(String message, Throwable cause) {
		super(message, cause);
	}
}

