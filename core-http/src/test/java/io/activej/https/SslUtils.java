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
class SslUtils {
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
}
