package io.activej.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Utils {

	public static long copy(InputStream from, OutputStream to) throws IOException {
		byte[] buf = new byte[16384];
		long total = 0L;

		while (true) {
			int r = from.read(buf);
			if (r == -1) {
				return total;
			}

			to.write(buf, 0, r);
			total += (long) r;
		}
	}

}
