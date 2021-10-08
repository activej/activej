import io.activej.inject.Injector;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.AbstractModule;

import java.net.InetSocketAddress;

public class MultiModuleExample {

	//[START REGION_1]
	public static class ConfigModule extends AbstractModule {
		@Provides
		InetSocketAddress inetSocketAddress() {
			return new InetSocketAddress(1234);
		}

		@Provides
		String dbName() {
			return "exampleDB";
		}
	}
	//[END REGION_1]

	//[START REGION_2]
	public static class ServiceModule extends AbstractModule {
		@Provides
		Service service(DataSource dataSource) {
			return new Service(dataSource);
		}
	}
	//[END REGION_2]

	//[START REGION_3]
	public static void main(String[] args) {
		Injector injector = Injector.of(new ConfigModule(), new ServiceModule());
		Service service = injector.getInstance(Service.class);
		service.process();
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

	//[START REGION_5]
	public static final class Service {
		private final DataSource dataSource;

		public Service(DataSource dataSource) {
			this.dataSource = dataSource;
		}

		public void process() {
			String data = dataSource.queryData();
			System.out.printf("Processing data: '%s'%n", data);
		}
	}
	//[END REGION_5]
}
