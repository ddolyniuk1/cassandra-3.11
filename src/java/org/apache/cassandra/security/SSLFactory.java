/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.security;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.io.util.FileUtils;
 
import javax.net.ssl.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class SSLFactory
{
    private static final Logger logger = LoggerFactory.getLogger(SSLFactory.class);
    private static boolean checkedExpiry = false;

    // Cache for SSL contexts
    private static final ConcurrentHashMap<String, CachedSSLContext> sslContextCache = new ConcurrentHashMap<>();

    // Hot reload configuration
    private static final int DEFAULT_HOT_RELOAD_INTERVAL_SECONDS = 600; // 10 minutes
    private static boolean hotReloadingInitialized = false;

    // Cached SSL context with last modified timestamps
    private static class CachedSSLContext {
        final SSLContext context;
        final long keystoreLastModified;
        final long truststoreLastModified;
        final EncryptionOptions options;

        CachedSSLContext(SSLContext context, long keystoreLastModified, long truststoreLastModified, EncryptionOptions options) {
            this.context = context;
            this.keystoreLastModified = keystoreLastModified;
            this.truststoreLastModified = truststoreLastModified;
            this.options = options;
        }

        boolean isOutdated() {
            File keystore = new File(options.keystore);
            File truststore = new File(options.truststore);
            return keystore.lastModified() > keystoreLastModified ||
                    truststore.lastModified() > truststoreLastModified;
        }
    }

    public static SSLServerSocket getServerSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = getSSLContext(options, true);
        SSLServerSocket serverSocket = (SSLServerSocket)ctx.getServerSocketFactory().createServerSocket();
        try
        {
            serverSocket.setReuseAddress(true);
            prepareSocket(serverSocket, options);
            serverSocket.bind(new InetSocketAddress(address, port), 500);
            return serverSocket;
        }
        catch (IllegalArgumentException | SecurityException | IOException e)
        {
            serverSocket.close();
            throw e;
        }
    }

    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException
    {
        SSLContext ctx = getSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port, localAddress, localPort);
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Create a socket and connect, using any local address */
    public static SSLSocket getSocket(EncryptionOptions options, InetAddress address, int port) throws IOException
    {
        SSLContext ctx = getSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket(address, port);
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Just create a socket */
    public static SSLSocket getSocket(EncryptionOptions options) throws IOException
    {
        SSLContext ctx = getSSLContext(options, true);
        SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket();
        try
        {
            prepareSocket(socket, options);
            return socket;
        }
        catch (IllegalArgumentException e)
        {
            socket.close();
            throw e;
        }
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLServerSocket serverSocket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(serverSocket.getSupportedCipherSuites(), options.cipher_suites);
        if(options.require_endpoint_verification)
        {
            SSLParameters sslParameters = serverSocket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            serverSocket.setSSLParameters(sslParameters);
        }
        serverSocket.setEnabledCipherSuites(suites);
        serverSocket.setNeedClientAuth(options.require_client_auth);
    }

    /** Sets relevant socket options specified in encryption settings */
    private static void prepareSocket(SSLSocket socket, EncryptionOptions options)
    {
        String[] suites = filterCipherSuites(socket.getSupportedCipherSuites(), options.cipher_suites);
        if(options.require_endpoint_verification)
        {
            SSLParameters sslParameters = socket.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            socket.setSSLParameters(sslParameters);
        }
        socket.setEnabledCipherSuites(suites);
    }

    public static synchronized void initHotReloading(ScheduledExecutorService executor)
    {
        if (hotReloadingInitialized)
        {
            logger.debug("SSL hot reloading is already initialized");
            return;
        }

        if (executor == null)
        {
            throw new IllegalArgumentException("Executor service cannot be null");
        }

        executor.scheduleWithFixedDelay(() -> {
            try
            {
                checkAndReloadCertificates(false);
            }
            catch (Exception e)
            {
                logger.error("Error during scheduled SSL certificate check", e);
            }
        }, DEFAULT_HOT_RELOAD_INTERVAL_SECONDS, DEFAULT_HOT_RELOAD_INTERVAL_SECONDS, TimeUnit.SECONDS);

        hotReloadingInitialized = true;
        logger.debug("SSL certificate hot reloading initialized with {} second interval",
                DEFAULT_HOT_RELOAD_INTERVAL_SECONDS);
    }

    public static void forceCheckCertificates() {
        try {
            checkAndReloadCertificates(true);
        } catch (Exception e) {
            logger.error("Error during forced SSL certificate check", e);
        }
    }

    private static void checkAndReloadCertificates(boolean force) {
        sslContextCache.forEach((key, cached) -> {
            if (force || cached.isOutdated()) {
                try {
                    logger.debug("Reloading SSL context for {}", key);
                    SSLContext newContext = getSSLContext(cached.options, true);
                    sslContextCache.put(key, new CachedSSLContext(
                            newContext,
                            new File(cached.options.keystore).lastModified(),
                            new File(cached.options.truststore).lastModified(),
                            cached.options
                    ));
                } catch (Exception e) {
                    logger.error("Failed to reload SSL context for {}", key, e);
                }
            }
        });
    }

    public static SSLContext getSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        String cacheKey = options.keystore + "|" + options.truststore;
        CachedSSLContext cached = sslContextCache.get(cacheKey);

        if (cached == null || cached.isOutdated()) {
            SSLContext context = createSSLContext(options, buildTruststore);
            cached = new CachedSSLContext(
                    context,
                    new File(options.keystore).lastModified(),
                    new File(options.truststore).lastModified(),
                    options
            );
            sslContextCache.put(cacheKey, cached);
        }

        return cached.context;
    }

    @SuppressWarnings("resource")
    private static SSLContext createSSLContext(EncryptionOptions options, boolean buildTruststore) throws IOException
    {
        FileInputStream tsf = null;
        FileInputStream ksf = null;
        SSLContext ctx;
        try
        {
            ctx = SSLContext.getInstance(options.protocol);
            TrustManager[] trustManagers = null;

            if(buildTruststore)
            {
                tsf = new FileInputStream(options.truststore);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(options.algorithm);
                KeyStore ts = KeyStore.getInstance(options.store_type);
                ts.load(tsf, options.truststore_password.toCharArray());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            ksf = new FileInputStream(options.keystore);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(options.algorithm);
            KeyStore ks = KeyStore.getInstance(options.store_type);
            ks.load(ksf, options.keystore_password.toCharArray());
            if (!checkedExpiry)
            {
                for (Enumeration<String> aliases = ks.aliases(); aliases.hasMoreElements(); )
                {
                    String alias = aliases.nextElement();
                    if (ks.getCertificate(alias).getType().equals("X.509"))
                    {
                        Date expires = ((X509Certificate) ks.getCertificate(alias)).getNotAfter();
                        if (expires.before(new Date()))
                            logger.warn("Certificate for {} expired on {}", alias, expires);
                    }
                }
                checkedExpiry = true;
            }
            kmf.init(ks, options.keystore_password.toCharArray());

            ctx.init(kmf.getKeyManagers(), trustManagers, null);

        }
        catch (Exception e)
        {
            throw new IOException("Error creating the initializing the SSL Context", e);
        }
        finally
        {
            FileUtils.closeQuietly(tsf);
            FileUtils.closeQuietly(ksf);
        }
        return ctx;
    }

    public static String[] filterCipherSuites(String[] supported, String[] desired)
    {
        if (Arrays.equals(supported, desired))
            return desired;
        List<String> ldesired = Arrays.asList(desired);
        ImmutableSet<String> ssupported = ImmutableSet.copyOf(supported);
        String[] ret = Iterables.toArray(Iterables.filter(ldesired, Predicates.in(ssupported)), String.class);
        if (desired.length > ret.length && logger.isWarnEnabled())
        {
            Iterable<String> missing = Iterables.filter(ldesired, Predicates.not(Predicates.in(Sets.newHashSet(ret))));
            logger.warn("Filtering out {} as it isn't supported by the socket", Iterables.toString(missing));
        }
        return ret;
    }
}