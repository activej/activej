package advanced.util;

import com.zaxxer.hikari.HikariConfig;
import io.activej.config.Config;
import io.activej.config.converter.ComplexConfigConverter;

import java.util.Map;

import static io.activej.config.Config.ifNotDefault;
import static io.activej.config.converter.ConfigConverters.*;

public final class HikariConfigConverter extends ComplexConfigConverter<HikariConfig> {
	public static HikariConfigConverter create() {
		return new HikariConfigConverter();
	}

	private String poolName;
	private boolean notSafeSql;
	private boolean allowMultiQueries;

	private HikariConfigConverter() {
		super(new HikariConfig());
	}

	public HikariConfigConverter withPoolName(String poolName) {
		this.poolName = poolName;
		return this;
	}

	public HikariConfigConverter withNotSafeSql() {
		this.notSafeSql = true;
		return this;
	}

	public HikariConfigConverter withAllowMultiQueries() {
		this.allowMultiQueries = true;
		return this;
	}

	private String getConnectionInitSql() {
		return notSafeSql ? "SET sql_safe_updates=0;" : "SET sql_safe_updates=1;";
	}

	@Override
	protected HikariConfig provide(Config config, HikariConfig defaultValue) {
		defaultValue.setRegisterMbeans(true);
		defaultValue.setPoolName(poolName);
		defaultValue.setConnectionInitSql(getConnectionInitSql());

		HikariConfig hikariConfig = new HikariConfig();
		if (allowMultiQueries) {
			hikariConfig.addDataSourceProperty("allowMultiQueries", "true");
		}
		config.apply(ofBoolean(), "autoCommit", defaultValue.isAutoCommit(), hikariConfig::setAutoCommit);
		config.apply(ofString(), "catalog", defaultValue.getCatalog(), hikariConfig::setCatalog);
		config.apply(ofString(), "connectionInitSql", defaultValue.getConnectionInitSql(), hikariConfig::setConnectionInitSql);
		config.apply(ofString(), "connectionTestQuery", defaultValue.getConnectionTestQuery(), hikariConfig::setConnectionTestQuery);
		config.apply(ofDurationAsMillis(), "connectionTimeout", defaultValue.getConnectionTimeout(), hikariConfig::setConnectionTimeout);
		config.apply(ofDurationAsMillis(), "validationTimeout", defaultValue.getValidationTimeout(), hikariConfig::setValidationTimeout);
		config.apply(ofString(), "dataSourceClassName", defaultValue.getDataSourceClassName(), hikariConfig::setDataSourceClassName);
		config.apply(ofString(), "driverClassName", defaultValue.getDriverClassName(), ifNotDefault(hikariConfig::setDriverClassName));
		config.apply(ofDurationAsMillis(), "idleTimeout", defaultValue.getIdleTimeout(), hikariConfig::setIdleTimeout);
		config.apply(ofLong(), "initializationFailTimeout", defaultValue.getInitializationFailTimeout(), hikariConfig::setInitializationFailTimeout);
		config.apply(ofBoolean(), "isolateInternalQueries", defaultValue.isIsolateInternalQueries(), hikariConfig::setIsolateInternalQueries);
		config.apply(ofString(), "jdbcUrl", defaultValue.getJdbcUrl(), hikariConfig::setJdbcUrl);
		config.apply(ofDurationAsMillis(), "leakDetectionThreshold", defaultValue.getLeakDetectionThreshold(), hikariConfig::setLeakDetectionThreshold);
		config.apply(ofInteger(), "maximumPoolSize", defaultValue.getMaximumPoolSize(), ifNotDefault(hikariConfig::setMaximumPoolSize));
		config.apply(ofDurationAsMillis(), "maxLifetime", defaultValue.getMaxLifetime(), hikariConfig::setMaxLifetime);
		config.apply(ofInteger(), "minimumIdle", defaultValue.getMinimumIdle(), ifNotDefault(hikariConfig::setMinimumIdle));
		config.apply(ofString(), "password", defaultValue.getPassword(), hikariConfig::setPassword);
		config.apply(ofString(), "poolName", defaultValue.getPoolName(), hikariConfig::setPoolName);
		config.apply(ofBoolean(), "readOnly", defaultValue.isReadOnly(), hikariConfig::setReadOnly);
		config.apply(ofBoolean(), "registerMbeans", defaultValue.isRegisterMbeans(), hikariConfig::setRegisterMbeans);
		config.apply(ofString(), "transactionIsolation", defaultValue.getTransactionIsolation(), hikariConfig::setTransactionIsolation);
		config.apply(ofString(), "username", defaultValue.getUsername(), hikariConfig::setUsername);
		Config propertiesConfig = config.getChild("extra");
		for (Map.Entry<String, Config> entry : propertiesConfig.getChildren().entrySet()) {
			hikariConfig.addDataSourceProperty(entry.getKey(), entry.getValue().getValue());
		}
		return hikariConfig;
	}
}
