module io.activej.trigger {
	requires transitive io.activej.jmx.stats;
	requires transitive io.activej.launcher;
	requires transitive io.activej.worker;

	exports io.activej.trigger;
	exports io.activej.trigger.util;

	opens io.activej.trigger to io.activej.inject;
}
