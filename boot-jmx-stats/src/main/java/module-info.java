module io.activej.jmx.stats {
	requires java.management;

	requires transitive io.activej.common;
	requires transitive io.activej.jmx.api;

	exports io.activej.jmx.stats;
}
