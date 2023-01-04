package io.activej.dns;

import io.activej.dns.protocol.*;
import io.activej.promise.Promises;
import io.activej.reactor.Reactor;
import io.activej.reactor.nio.NioReactor;
import io.activej.test.rules.ActivePromisesRule;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static io.activej.dns.protocol.DnsProtocol.ResponseErrorCode.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.*;

@Ignore
public final class ReactiveDnsClientTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private CachedDnsClient cachedDnsClient;
	private static final int DNS_SERVER_PORT = 53;

	private static final InetSocketAddress UNREACHABLE_DNS = new InetSocketAddress("8.0.8.8", DNS_SERVER_PORT);
	private static final InetSocketAddress LOCAL_DNS = new InetSocketAddress("192.168.0.1", DNS_SERVER_PORT);

	@Before
	public void setUp() {
		NioReactor reactor = Reactor.getCurrentReactor();
		cachedDnsClient = CachedDnsClient.create(reactor, ReactiveDnsClient.create(reactor).withDnsServerAddress(LOCAL_DNS));
	}

	@Test
	public void testCachedDnsClient() throws Exception {
		DnsQuery query = DnsQuery.ipv4("www.google.com");
		InetAddress[] ips = {InetAddress.getByName("173.194.113.210"), InetAddress.getByName("173.194.113.209")};

		cachedDnsClient.getCache().add(query, DnsResponse.of(DnsTransaction.of((short) 0, query), DnsResourceRecord.of(ips, 10)));
		DnsResponse result = await(cachedDnsClient.resolve4("www.google.com"));

		assertNotNull(result.getRecord());
		System.out.println(Arrays.stream(result.getRecord().getIps())
				.map(InetAddress::toString)
				.collect(joining(", ", "Resolved: ", ".")));
	}

	@Test
	public void testCachedDnsClientError() {
		DnsQuery query = DnsQuery.ipv4("www.google.com");

		cachedDnsClient.getCache().add(query, DnsResponse.ofFailure(DnsTransaction.of((short) 0, query), SERVER_FAILURE));

		DnsQueryException e = awaitException(cachedDnsClient.resolve4("www.google.com"));
		assertEquals(SERVER_FAILURE, e.getResult().getErrorCode());
	}

	@Test
	public void testDnsClient() {
		DnsClient dnsClient = ReactiveDnsClient.create(Reactor.getCurrentReactor());

		List<DnsResponse> list = await(Promises.toList(Stream.of("www.google.com", "www.github.com", "www.kpi.ua")
				.map(dnsClient::resolve4)));

		assertFalse(list.isEmpty());
	}

	@Test
	public void testDnsClientTimeout() {
		DnsClient dnsClient = ReactiveDnsClient.create(Reactor.getCurrentReactor())
				.withTimeout(Duration.ofMillis(20))
				.withDnsServerAddress(UNREACHABLE_DNS);

		DnsQueryException e = awaitException(dnsClient.resolve4("www.google.com"));
		assertEquals(TIMED_OUT, e.getResult().getErrorCode());
	}

	@Test
	public void testDnsNameError() {
		DnsClient dnsClient = ReactiveDnsClient.create(Reactor.getCurrentReactor());

		DnsQueryException e = awaitException(dnsClient.resolve4("example.ensure-such-top-domain-it-will-never-exist"));
		assertEquals(NAME_ERROR, e.getResult().getErrorCode());
	}

	@Test
	public void testDnsLabelSize() {
		DnsClient dnsClient = ReactiveDnsClient.create(Reactor.getCurrentReactor());

		String domainName = "example.huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label-huge-dns-label.com";
		IllegalArgumentException e = awaitException(dnsClient.resolve4(domainName));
		assertEquals("Label size cannot exceed 63 octets", e.getMessage());
	}

}
