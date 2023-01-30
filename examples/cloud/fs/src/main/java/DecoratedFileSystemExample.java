import io.activej.bytebuf.ByteBuf;
import io.activej.config.Config;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.fs.ForwardingFileSystem;
import io.activej.fs.IFileSystem;
import io.activej.fs.tcp.FileSystemServer;
import io.activej.inject.annotation.Eager;
import io.activej.inject.annotation.Named;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launcher.Launcher;
import io.activej.promise.Promise;
import io.activej.reactor.nio.NioReactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.activej.launchers.fs.Initializers.ofFileSystemServer;

/**
 * This example demonstrates using Decorator pattern to add extra functionality to ActiveFs instance
 */
public class DecoratedFileSystemExample extends ServerSetupExample {

	//[START OVERRIDE]
	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Eager
			@Provides
			FileSystemServer fileSystemServer(NioReactor reactor, @Named("decorated") IFileSystem decoratedFS, Config config) {
				return FileSystemServer.builder(reactor, decoratedFS)
						.initialize(ofFileSystemServer(config.getChild("asyncfs")))
						.build();
			}

			@Provides
			@Named("decorated")
			IFileSystem decoratedFileSystem(IFileSystem fs) {
				return new FileSystem_Logging(fs);
			}
		};
	}
	//[END OVERRIDE]

	public static void main(String[] args) throws Exception {
		Launcher launcher = new DecoratedFileSystemExample();
		launcher.launch(args);
	}

	//[START WRAPPER]
	private static final class FileSystem_Logging extends ForwardingFileSystem {
		private static final Logger logger = LoggerFactory.getLogger(FileSystem_Logging.class);

		public FileSystem_Logging(IFileSystem peer) {
			super(peer);
		}

		@Override
		public Promise<ChannelConsumer<ByteBuf>> upload(String name, long size) {
			return super.upload(name)
					.map(consumer -> {
						logger.info("Starting upload of file: {}. File size is {} bytes", name, size);
						return consumer
								.withAcknowledgement(ack -> ack
										.whenResult(() -> logger.info("Upload of file {} finished", name)));
					});
		}

		@Override
		public Promise<ChannelSupplier<ByteBuf>> download(String name, long offset, long limit) {
			return super.download(name, offset, limit)
					.map(supplier -> {
						logger.info("Starting downloading file: {}", name);
						return supplier
								.withEndOfStream(eos -> eos
										.whenResult(() -> logger.info("Download of file {} finished", name)));
					});

		}
	}
	//[END WRAPPER]
}
