module io.activej.jmx {
	requires java.management;

	requires transitive io.activej.bytebuf;
	requires transitive io.activej.trigger;

	exports io.activej.jmx;

	opens io.activej.jmx to io.activej.inject;
}
