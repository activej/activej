import io.activej.bytebuf.ByteBufStrings;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.file.ChannelFileReader;
import io.activej.csp.file.ChannelFileWriter;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class ChannelFileExample {
	private static final ExecutorService executor = newSingleThreadExecutor();
	private static final Eventloop eventloop = Eventloop.create().withCurrentThread();
	private static final Path PATH;

	static {
		try {
			PATH = Files.createTempFile("NewFile", ".txt");
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	//[START REGION_1]
	private static @NotNull Promise<Void> writeToFile() {
		return ChannelSupplier.of(
						ByteBufStrings.wrapAscii("Hello, this is example file\n"),
						ByteBufStrings.wrapAscii("This is the second line of file\n"))
				.streamTo(ChannelFileWriter.open(executor, PATH, WRITE));
	}

	private static @NotNull Promise<Void> readFile() {
		return ChannelFileReader.open(executor, PATH)
				.then(cfr -> cfr.streamTo(ChannelConsumer.ofConsumer(buf -> System.out.print(buf.asString(UTF_8)))));

	}
	//[END REGION_1]

	public static void main(String[] args) {
		Promises.sequence(
				ChannelFileExample::writeToFile,
				ChannelFileExample::readFile);

		eventloop.run();
		executor.shutdown();
	}
}
