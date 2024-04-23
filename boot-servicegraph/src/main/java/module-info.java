module io.activej.service {
	requires java.sql;
	requires io.activej.net;

	requires transitive io.activej.jmx.api;
	requires transitive io.activej.worker;
	requires transitive io.activej.launcher;

	exports io.activej.service;
	exports io.activej.service.adapter;

	opens io.activej.service to io.activej.inject;
}
