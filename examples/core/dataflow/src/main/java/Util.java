import java.io.IOException;
import java.nio.file.Files;

public final class Util {
	public static String createTempDir(String name) {
		try {
			return Files.createTempDirectory(name).toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
