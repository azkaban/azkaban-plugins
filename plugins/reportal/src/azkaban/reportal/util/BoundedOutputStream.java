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
