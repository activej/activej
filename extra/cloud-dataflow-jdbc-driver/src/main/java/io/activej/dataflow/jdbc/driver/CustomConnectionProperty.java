package io.activej.dataflow.jdbc.driver;

import org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.hc.core5.http.io.SocketConfig;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

public enum CustomConnectionProperty implements ConnectionProperty {
	SOCKET_TIMEOUT("socket_timeout", Type.NUMBER, SocketConfig.DEFAULT.getSoTimeout().toMilliseconds());

	private final String camelName;
	private final Type type;
	private final Object defaultValue;

	private static final Map<String, CustomConnectionProperty> NAME_TO_PROPS;

	static {
		NAME_TO_PROPS = new HashMap<>();
		for (CustomConnectionProperty p : CustomConnectionProperty.values()) {
			NAME_TO_PROPS.put(p.camelName.toUpperCase(Locale.ROOT), p);
			NAME_TO_PROPS.put(p.name(), p);
		}
	}

	CustomConnectionProperty(String camelName,
	                         Type type,
	                         Object defaultValue) {
		this.camelName = camelName;
		this.type = type;
		this.defaultValue = defaultValue;
		assert type.valid(defaultValue, type.defaultValueClass());
	}

	@Override
	public String camelName() {
		return camelName;
	}

	@Override
	public Object defaultValue() {
		return defaultValue;
	}

	@Override
	public Type type() {
		return type;
	}

	@Override
	public Class<?> valueClass() {
		return type.defaultValueClass();
	}

	@Override
	public PropEnv wrap(Properties properties) {
		return new PropEnv(parse(properties, NAME_TO_PROPS), this);
	}

	@Override
	public boolean required() {
		return false;
	}
}
