package io.activej.https;

import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

@SuppressWarnings("SameParameterValue")
public class SslUtils {
	private static final String KEYSTORE_PATH = "./src/test/resources/keystore.jks";
	private static final String KEYSTORE_PASS = "testtest";
	private static final String KEY_PASS = "testtest";

	private static final String TRUSTSTORE_PATH = "./src/test/resources/truststore.jks";
	private static final String TRUSTSTORE_PASS = "testtest";

	static TrustManager[] createTrustManagers(File path, String pass) throws Exception {
		KeyStore trustStore = KeyStore.getInstance("JKS");

		try (InputStream trustStoreIS = new FileInputStream(path)) {
			trustStore.load(trustStoreIS, pass.toCharArray());
		}
		TrustManagerFactory trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		trustFactory.init(trustStore);
		return trustFactory.getTrustManagers();
	}

	static KeyManager[] createKeyManagers(File path, String storePass, String keyPass) throws Exception {
		KeyStore store = KeyStore.getInstance("JKS");
		try (InputStream is = new FileInputStream(path)) {
			store.load(is, storePass.toCharArray());
		}
		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(store, keyPass.toCharArray());
		return kmf.getKeyManagers();
	}

	static SSLContext createSslContext(String algorithm, KeyManager[] keyManagers, TrustManager[] trustManagers,
			SecureRandom secureRandom) throws NoSuchAlgorithmException, KeyManagementException {
		SSLContext instance = SSLContext.getInstance(algorithm);
		instance.init(keyManagers, trustManagers, secureRandom);
		return instance;
	}

	public static SSLContext createTestSslContext() {
		try {
			SSLContext instance = SSLContext.getInstance("TLSv1.2");

			KeyStore keyStore = KeyStore.getInstance("JKS");
			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			try (InputStream input = new FileInputStream(KEYSTORE_PATH)) {
				keyStore.load(input, KEYSTORE_PASS.toCharArray());
			}
			kmf.init(keyStore, KEY_PASS.toCharArray());

			KeyStore trustStore = KeyStore.getInstance("JKS");
			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			try (InputStream input = new FileInputStream(TRUSTSTORE_PATH)) {
				trustStore.load(input, TRUSTSTORE_PASS.toCharArray());
			}
			tmf.init(trustStore);

			instance.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
			return instance;
		} catch (Exception e) {
			throw new AssertionError(e);
		}
	}
}
