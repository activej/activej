/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.dns.protocol;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufPool;
import io.activej.common.exception.InvalidSizeException;
import io.activej.common.exception.MalformedDataException;
import io.activej.common.exception.UnknownFormatException;
import org.jetbrains.annotations.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.activej.dns.protocol.DnsProtocol.ResponseErrorCode.NO_DATA;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * This class allows to use a simple subset of the Domain Name System (or DNS) protocol
 */
public final class DnsProtocol {
	private static final int MAX_SIZE = 512;

	private static final byte[] STANDARD_QUERY_HEADER = {
			0x01, 0x00, // flags: 0x0100 - standard query
			0x00, 0x01, // number of questions      : 1
			0x00, 0x00, // number of answer RRs     : 0
			0x00, 0x00, // number of authority RRs  : 0
			0x00, 0x00, // number of additional RRs : 0
	};

	private static final AtomicInteger xorshiftState = new AtomicInteger(1);

	/* atomic 16-bit xorshift LSFR, cycling through all 16-bit values except zero */
	public static short generateTransactionId() {
		return (short) xorshiftState.updateAndGet(old -> {
			short x = (short) old;
			x ^= (x & 0xffff) << 7;
			x ^= (x & 0xffff) >>> 9;
			x ^= (x & 0xffff) << 8;
			assert x != 0 : "Xorshift LFSR can never produce zero";
			return x;
		});
	}

	/**
	 * Creates a bytebuf with a DNS query payload
	 *
	 * @param transaction DNS transaction to encode
	 * @return ByteBuf with DNS message payload data
	 */
	public static ByteBuf createDnsQueryPayload(DnsTransaction transaction) {
		ByteBuf byteBuf = ByteBufPool.allocate(MAX_SIZE);

		byteBuf.writeShort(transaction.getId());
		// standard query flags, 1 question and 0 of other stuff
		byteBuf.write(STANDARD_QUERY_HEADER);

		// query domain name
		byte componentSize = 0;
		DnsQuery query = transaction.getQuery();
		byte[] domainBytes = query.getDomainName().getBytes(US_ASCII);

		int pos = -1;
		while (++pos < domainBytes.length) {
			if (domainBytes[pos] != '.') {
				componentSize++;
				continue;
			}
			byteBuf.writeByte(componentSize);
			byteBuf.write(domainBytes, pos - componentSize, componentSize);
			componentSize = 0;
		}
		byteBuf.writeByte(componentSize);
		byteBuf.write(domainBytes, pos - componentSize, componentSize);
		byteBuf.writeByte((byte) 0x0); // terminator byte

		// query record type
		byteBuf.writeShort(query.getRecordType().getCode());
		// query class: IN
		byteBuf.writeShort(QueryClass.INTERNET.getCode());
		return byteBuf;
	}

	/**
	 * Reads a DNS query response from payload
	 *
	 * @param payload byte buffer with response payload
	 * @return DNS query response parsed from the payload
	 * @throws MalformedDataException when parsing fails
	 */
	public static DnsResponse readDnsResponse(ByteBuf payload) throws MalformedDataException {
		try {
			short transactionId = payload.readShort();
			payload.moveHead(1); // skip first flags byte

			//                                                                    last 4 flag bits are error code
			ResponseErrorCode errorCode = ResponseErrorCode.fromBits(payload.readByte() & 0b00001111);

			short questionCount = payload.readShort();
			short answerCount = payload.readShort();
			payload.moveHead(4); // skip authority and additional counts (2 bytes each)

			if (questionCount != 1) {
				// malformed response, we are always sending only one question
				throw new MalformedDataException("Received DNS response has question count not equal to one");
			}

			// read domain name from first query
			StringBuilder sb = new StringBuilder();
			byte componentSize = payload.readByte();
			while (componentSize != 0) {
				sb.append(new String(payload.array(), payload.head(), componentSize, US_ASCII));
				payload.moveHead(componentSize);
				componentSize = payload.readByte();
				if (componentSize != 0) {
					sb.append('.');
				}
			}
			String domainName = sb.toString();

			// read query record type
			short recordTypeCode = payload.readShort();
			RecordType recordType = RecordType.fromCode(recordTypeCode);
			if (recordType == null) {
				// malformed response, we are sending query only with existing RecordType's
				throw new UnknownFormatException("Received DNS response with unknown query record type (" +
						Integer.toHexString(recordTypeCode & 0xFFFF) + ")");
			}

			// read query class (only for sanity check)
			short queryClassCode = payload.readShort();
			QueryClass queryClass = QueryClass.fromCode(queryClassCode);
			if (queryClass != QueryClass.INTERNET) {
				throw new UnknownFormatException("Received DNS response with unknown query class (" +
						Integer.toHexString(queryClassCode & 0xFFFF) + ")");
			}

			// at this point, we know the query of this response
			DnsQuery query = DnsQuery.of(domainName, recordType);

			// and so we have all the data to fail if error code is not zero
			DnsTransaction transaction = DnsTransaction.of(transactionId, query);
			if (errorCode != ResponseErrorCode.NO_ERROR) {
				return DnsResponse.ofFailure(transaction, errorCode);
			}

			List<InetAddress> ips = new ArrayList<>();
			int minTtl = Integer.MAX_VALUE;
			for (int i = 0; i < answerCount; i++) {

				// check for message compression (RFC 1035 section 4.1.4. Message compression, https://tools.ietf.org/rfc/rfc1035#section-4.1.4)
				byte b = payload.readByte();
				while (b != 0) {
					int twoBits = (b & 0xFF) >> 6;
					if (twoBits == 0b11) {
						payload.moveHead(1); // skip domain pointer (second byte of 2 bytes)
						break;
					} else if (twoBits == 0b00) {
						// skip the fqdn
						payload.moveHead(b);
						b = payload.readByte();
					} else {
						throw new MalformedDataException("Unsupported compression method");
					}
				}

				RecordType currentRecordType = RecordType.fromCode(payload.readShort());
				payload.moveHead(2); // skip answer class (2 bytes)
				if (currentRecordType != recordType) { // this is some other record
					payload.moveHead(4); // skip ttl
					payload.moveHead(payload.readShort()); // and skip data
					continue;
				}
				minTtl = Math.min(payload.readInt(), minTtl);
				short length = payload.readShort();
				if (length != recordType.dataLength) {
					throw new InvalidSizeException("Bad record length received. " + recordType +
							"-record length should be " + recordType.dataLength + " bytes, it was " + length);
				}
				byte[] bytes = new byte[length];
				payload.read(bytes);
				try {
					ips.add(InetAddress.getByAddress(domainName, bytes));
				} catch (UnknownHostException ignored) {
					// never happens, we have explicit check for length
				}
			}
			// fail if no IPs were parsed (or answer count was zero)
			if (ips.isEmpty()) {
				return DnsResponse.ofFailure(transaction, NO_DATA);
			}
			return DnsResponse.of(transaction, DnsResourceRecord.of(ips.toArray(new InetAddress[0]), minTtl));
		} catch (IndexOutOfBoundsException e) {
			throw new MalformedDataException("Failed parsing DNS response", e);
		}
	}

	// used DNS enum types:

	public enum RecordType {
		A(0x0001, 4),
		AAAA(0x001C, 16);

		private final short code;
		private final short dataLength;

		RecordType(int code, int dataLength) {
			this.code = (short) code;
			this.dataLength = (short) dataLength;
		}

		public short getCode() {
			return code;
		}

		public short getDataLength() {
			return dataLength;
		}

		/**
		 * Returns record type based on its code or <code>null</code> if code is invalid.
		 */
		static @Nullable RecordType fromCode(short code) {
			switch (code) {
				case 0x0001:
					return A;
				case 0x001C:
					return AAAA;
				default:
					return null;
			}
		}
	}

	public enum QueryClass {
		INTERNET(0x0001);

		private final short code;

		QueryClass(int code) {
			this.code = (short) code;
		}

		public short getCode() {
			return code;
		}

		/**
		 * Returns query class based on its code or <code>null</code> if code is invalid.
		 */
		public static @Nullable QueryClass fromCode(short code) {
			if (code == 0x0001) return INTERNET;
			return null;
		}
	}

	/**
	 * Handled error codes:
	 * <table>
	 * <caption>RFC6895 - DNS, page 5:</caption>
	 * <tr><th>RCODE:</th><th>NAME:</th><th>Description:</th><th>Reference:</th></tr>
	 * <tr><td>0</td><td>NoError</td><td>No Error</td><td>RFC1035</td></tr>
	 * <tr><td>1</td><td>FormErr</td><td>Format Error</td><td>RFC1035</td></tr>
	 * <tr><td>2</td><td>ServFail</td><td>Server Failure</td><td>RFC1035</td></tr>
	 * <tr><td>3</td><td>NXDomain</td><td>Non-Existent Domain</td><td>RFC1035</td></tr>
	 * <tr><td>4</td><td>NotImp</td><td>Not Implemented</td><td>RFC1035</td></tr>
	 * <tr><td>5</td><td>Refused</td><td>Query Refused</td><td>RFC1035</td></tr>
	 * <tr>...</tr>
	 * </table>
	 */
	public enum ResponseErrorCode {
		NO_ERROR,
		FORMAT_ERROR,
		SERVER_FAILURE,
		NAME_ERROR,
		NOT_IMPLEMENTED,
		REFUSED,
		// custom error codes
		NO_DATA,
		TIMED_OUT,
		UNKNOWN;

		static ResponseErrorCode fromBits(int rcodeBits) {
			if (rcodeBits < 0 || rcodeBits > 5) {
				return UNKNOWN;
			}
			return values()[rcodeBits];
		}
	}
}
