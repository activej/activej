import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.ModuleBuilder;

import java.net.InetSocketAddress;

public class ClassScanExample {

	//[START REGION_1]
	public static class StaticProviders {
		@Provides
		static int port() {
			return 1234;
		}

		@Provides
		static String databaseName() {
			return "exampleDB";
		}
	}
	//[END REGION_1]

	//[START REGION_2]
	public static class MixedProviders {
		private final String hostname;

		public MixedProviders(String hostname) {
			this.hostname = hostname;
		}

		@Provides
		InetSocketAddress address(int port) {
			return new InetSocketAddress(hostname, port);
		}

		@Provides
		static DataSource dataSource(InetSocketAddress address, String dbName) {
			return new DataSource(address, dbName);
		}
	}
	//[END REGION_2]

	//[START REGION_3]
	public static void main(String[] args) {
		Injector injector = Injector.of(
			ModuleBuilder.create()
				.scan(StaticProviders.class)
				.scan(new MixedProviders("example.com"))
				.build()
		);

		DataSource dataSource = injector.getInstance(DataSource.class);
		System.out.println(dataSource.queryData());
	}
	//[END REGION_3]

	//[START REGION_4]
	public static final class DataSource {
		private final InetSocketAddress address;
		private final String dbName;

		@Inject
		public DataSource(InetSocketAddress address, String dbName) {
			this.address = address;
			this.dbName = dbName;
		}

		public String queryData() {
			System.out.printf("Querying %s:%s for data%n", address, dbName);
			return "data";
		}
	}
	//[END REGION_4]
}
