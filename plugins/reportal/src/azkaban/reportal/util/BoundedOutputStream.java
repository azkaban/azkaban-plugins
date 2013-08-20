package azkaban.reportal.util;

import java.io.IOException;
import java.io.OutputStream;

public class BoundedOutputStream extends OutputStream {
	OutputStream out;
	int size = Integer.MAX_VALUE;

	public BoundedOutputStream(OutputStream out, int size) {
		this.out = out;
		this.size = size;
	}

	@Override
	public void flush() throws IOException {
		out.flush();
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

	@Override
	public void write(byte[] b) throws IOException {
		if (size <= 0)
			return;
		if (size - b.length < 0) {
			out.write(b, 0, b.length - size);
			size = 0;
		}
		else {
			out.write(b);
			size -= b.length;
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		if (size <= 0)
			return;
		if (size - len < 0) {
			out.write(b, off, len - size);
			size = 0;
		}
		else {
			out.write(b, off, len);
			size -= len;
		}
	}

	@Override
	public void write(int b) throws IOException {
		if (size <= 0)
			return;
		out.write(b);
		size--;
	}

}
