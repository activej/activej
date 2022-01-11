package discovery;

import io.activej.async.function.AsyncSupplier;
import io.activej.common.StringFormatUtils;
import io.activej.common.exception.MalformedDataException;
import io.activej.config.Config;
import io.activej.crdt.CrdtStorageClient;
import io.activej.crdt.storage.CrdtStorage;
import io.activej.crdt.storage.cluster.DiscoveryService;
import io.activej.crdt.storage.cluster.RendezvousPartitionings;
import io.activej.crdt.storage.cluster.RendezvousPartitionings.Partitioning;
import io.activej.crdt.util.CrdtDataSerializer;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.activej.promise.SettablePromise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.InetSocketAddress;
import java.util.*;

import static io.activej.common.Checks.checkArgument;
import static io.activej.common.Utils.difference;
import static io.activej.config.converter.ConfigConverters.*;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public final class ConfigDiscoveryService<K extends Comparable<K>, S> implements DiscoveryService<K, S, String> {
	private final Partitionings<K, S, String> partitionings;

	private ConfigDiscoveryService(Partitionings<K, S, String> partitionings) {
		this.partitionings = partitionings;
	}

	public static <K extends Comparable<K>, S> ConfigDiscoveryService<K, S> create(
			Eventloop eventloop,
			CrdtStorage<K, S> localStorage,
			CrdtDataSerializer<K, S> crdtDataSerializer,
			Config config) {
		return doCreate(eventloop, config, localStorage, crdtDataSerializer);
	}

	public static <K extends Comparable<K>, S> ConfigDiscoveryService<K, S> createForRpcStrategy(
			Eventloop eventloop,
			Config config) {
		return doCreate(eventloop, config, null, null);
	}

	@NotNull
	private static <K extends Comparable<K>, S> ConfigDiscoveryService<K, S> doCreate(Eventloop eventloop,
			Config config, @Nullable CrdtStorage<K, S> localStorage, @Nullable CrdtDataSerializer<K, S> crdtDataSerializer) {
		Collection<Config> partitioningsConfig = config.getChild("partitionings").getChildren().values();

		Set<String> allIds = new HashSet<>();
		List<Partitioning<String>> partitionings = new ArrayList<>();
		for (Config partitioning : partitioningsConfig) {
			Set<String> ids = new HashSet<>(partitioning.get(ofList(ofString()), "ids"));
			checkArgument(!ids.isEmpty(), "Empty partitioning ids");

			allIds.addAll(ids);

			int replicas = partitioning.get(ofInteger(), "replicas", 1);
			boolean repartition = partitioning.get(ofBoolean(), "repartition", false);
			boolean active = partitioning.get(ofBoolean(), "active", false);

			partitionings.add(Partitioning.create(ids, replicas, repartition, active));
		}

		if (localStorage == null) {
			return toDiscoveryService(partitionings, emptyMap());
		}

		List<String> addressStrings = config.get(ofList(ofString()), "addresses", emptyList());
		Map<String, CrdtStorage<K, S>> partitions = new HashMap<>();

		boolean localPut = false;
		for (String addressString : addressStrings) {
			int splitIdx = addressString.lastIndexOf('=');
			checkArgument(splitIdx != -1, "Wrong address format");

			String id = addressString.substring(0, splitIdx);
			String address = addressString.substring(splitIdx + 1);

			if (address.equals("local")) {
				localPut = true;
				partitions.put(id, localStorage);
			} else {
				InetSocketAddress socketAddress;
				try {
					socketAddress = StringFormatUtils.parseInetSocketAddress(address);
				} catch (MalformedDataException e) {
					throw new IllegalArgumentException(e);
				}

				CrdtStorageClient<K, S> client = CrdtStorageClient.create(eventloop, socketAddress, crdtDataSerializer);
				partitions.put(id, client);
			}
		}

		checkArgument(localPut, "Local partition is not used");

		Set<String> missingAddresses = difference(allIds, partitions.keySet());
		checkArgument(missingAddresses.isEmpty(), "There are partitions with missing addresses: " + missingAddresses);

		Set<String> danglingAddresses = difference(partitions.keySet(), allIds);
		checkArgument(danglingAddresses.isEmpty(), "There are dangling addresses: " + danglingAddresses);

		return toDiscoveryService(partitionings, partitions);
	}

	private static <K extends Comparable<K>, S> ConfigDiscoveryService<K, S> toDiscoveryService(
			List<Partitioning<String>> partitionings,
			Map<String, CrdtStorage<K, S>> partitions
	) {
		RendezvousPartitionings<K, S, String> rendezvousPartitionings = RendezvousPartitionings.create(partitions);
		for (Partitioning<String> partitioning : partitionings) {
			rendezvousPartitionings = rendezvousPartitionings.withPartitioning(partitioning);
		}

		return new ConfigDiscoveryService<>(rendezvousPartitionings);
	}

	@Override
	public AsyncSupplier<Partitionings<K, S, String>> discover() {
		return new AsyncSupplier<Partitionings<K, S, String>>() {
			boolean retrieved;

			@Override
			public Promise<Partitionings<K, S, String>> get() {
				if (!retrieved) {
					retrieved = true;
					return Promise.of(partitionings);
				}
				return new SettablePromise<>();
			}
		};
	}
}
