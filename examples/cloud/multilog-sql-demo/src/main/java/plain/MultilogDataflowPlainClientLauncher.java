package plain;

import io.activej.config.Config;
import io.activej.dataflow.ISqlDataflow;
import io.activej.dataflow.exception.DataflowException;
import io.activej.datastream.StreamConsumer_ToList;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;
import io.activej.inject.module.Module;
import io.activej.launchers.dataflow.DataflowClientLauncher;
import io.activej.reactor.Reactor;
import io.activej.record.Record;
import misc.PrintUtils;
import module.MultilogDataflowClientModule;
import org.apache.calcite.runtime.CalciteException;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public final class MultilogDataflowPlainClientLauncher extends DataflowClientLauncher {

	@Inject
	Reactor reactor;

	@Inject
	ISqlDataflow sqlDataflow;

	@Override
	protected Module getBusinessLogicModule() {
		return MultilogDataflowClientModule.create();
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() {
				return Config.create()
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
			}
		};
	}

	@Override
	protected void onStart() throws Exception {
		executeQuery("SELECT 1");
		System.out.println("Connection to partitions established");
	}

	@Override
	protected void run() throws Exception {
		Scanner scanIn = new Scanner(System.in);

		while (true) {
			System.out.println("Enter your query:");
			System.out.print("> ");
			String query = scanIn.nextLine();
			if (query.isEmpty()) {
				System.out.println("Exiting...");
				break;
			}

			List<Record> records;
			try {
				records = executeQuery(query);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw e;
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception &&
						!(cause instanceof RuntimeException || cause instanceof DataflowException) ||
						cause instanceof CalciteException) {
					// recoverable
					System.err.println("WARNING: " + cause.getMessage());
					continue;
				} else {
					throw new RuntimeException(cause);
				}
			}

			System.out.println("\n\n");
			PrintUtils.printRecords(records);
		}
	}

	private List<Record> executeQuery(String query) throws InterruptedException, ExecutionException {
		return reactor.submit(() -> {
					StreamConsumer_ToList<Record> consumer = StreamConsumer_ToList.create();
					return sqlDataflow.query(query)
							.then(supplier -> supplier.streamTo(consumer))
							.map($ -> consumer.getList());
				})
				.get();
	}

	public static void main(String[] args) throws Exception {
		new MultilogDataflowPlainClientLauncher().launch(args);
	}
}
