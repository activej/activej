package plain;

import io.activej.config.Config;
import io.activej.dataflow.SqlDataflow;
import io.activej.datastream.StreamConsumerToList;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.dataflow.DataflowClientLauncher;
import io.activej.record.Record;
import misc.PrintUtils;
import module.MultilogDataflowClientModule;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public final class MultilogDataflowPlainClientLauncher extends DataflowClientLauncher {

	@Inject
	Eventloop eventloop;

	@Inject
	SqlDataflow sqlDataflow;

	@Override
	protected Module getBusinessLogicModule() {
		return MultilogDataflowClientModule.create();
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() throws IOException {
				return Config.create()
						.with("dataflow.secondaryBufferPath", Files.createTempDirectory("secondaryBufferPath").toString())
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
			}
		};
	}

	@Override
	protected void run() throws Exception {

		Scanner scanIn = new Scanner(System.in);
		while (true) {
			System.out.print("Enter your query:");
			System.out.print("> ");
			String query = scanIn.nextLine();
			if (query.isEmpty()) {
				System.out.println("Exiting...");
				break;
			}

			try {
				executeQuery(query);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception && !(cause instanceof RuntimeException)) {
					// recoverable
					e.printStackTrace();
				} else {
					throw new RuntimeException(cause);
				}
			}
		}
	}

	private void executeQuery(String query) throws InterruptedException, ExecutionException {
		List<Record> records = eventloop.submit(() -> {
					StreamConsumerToList<Record> consumer = StreamConsumerToList.create();
					return sqlDataflow.query(query)
							.then(supplier -> supplier.streamTo(consumer))
							.map($ -> consumer.getList());
				})
				.get();

		System.out.println("\n\n");
		PrintUtils.printRecords(records);
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowPlainClientLauncher().launch(args);
	}
}
