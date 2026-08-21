/*
 * Copyright © 2015 Integrated Knowledge Management (support@ikm.dev)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.ikm.tinkar.provider.grpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Where to reach the gRPC service, and over which transport.
 *
 * <p>Deliberately free of dependencies beyond {@link GrpcTlsConfig} and a logger: this is a
 * pure function over a string, and keeping it out of
 * {@code GrpcPrimitiveDataService.Controller} means parsing can be exercised without loading
 * that class — whose static initializer pulls in the entity stack to resolve bootstrap
 * concepts.
 *
 * @param host the hostname to dial
 * @param port the port to dial
 * @param tls  the transport implied by the URL, layered over the configured default
 */
public record GrpcServiceTarget(String host, int port, GrpcTlsConfig tls) {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcServiceTarget.class);

    /** Host used when the URL names none. */
    public static final String DEFAULT_HOST = "localhost";

    /** Port used when the URL names none, or names one that will not parse. */
    public static final int DEFAULT_PORT = 9095;

    /**
     * Parses a service URL into an address plus transport.
     *
     * <p>An optional scheme selects the transport and is authoritative when present, so the
     * Service URL field alone can move a user between plaintext and TLS:
     * <ul>
     *   <li>{@code grpcs://host:443} or {@code https://…} — TLS</li>
     *   <li>{@code grpc://host:9095} or {@code http://…} — plaintext</li>
     *   <li>{@code host:9095} — whatever {@code baseTls} says, i.e. the
     *       {@code komet.grpc.tls*} system properties</li>
     * </ul>
     * A CA path or authority override in {@code baseTls} is carried into the TLS case,
     * because those describe <em>how</em> to verify rather than <em>whether</em> to use TLS.
     *
     * <p>The scheme is stripped before host and port are split. Splitting first on the last
     * {@code ':'} would take {@code grpcs://host:9095} apart into host {@code "grpcs://host"},
     * which then fails to resolve with nothing in the error pointing at the cause.
     *
     * @param rawUrl  the configured URL; null, blank, and malformed input fall back to defaults
     * @param baseTls transport settings to start from
     * @return the parsed target, never null
     */
    public static GrpcServiceTarget parse(String rawUrl, GrpcTlsConfig baseTls) {
        String url = rawUrl == null ? "" : rawUrl.trim();
        GrpcTlsConfig tls = baseTls;

        int schemeEnd = url.indexOf("://");
        if (schemeEnd > 0) {
            String scheme = url.substring(0, schemeEnd);
            url = url.substring(schemeEnd + 3).trim();
            if (scheme.equalsIgnoreCase("grpcs") || scheme.equalsIgnoreCase("https")) {
                tls = tls.withTlsEnabled();
            } else if (scheme.equalsIgnoreCase("grpc") || scheme.equalsIgnoreCase("http")) {
                tls = GrpcTlsConfig.disabled();
            } else {
                LOG.warn("Unrecognised scheme '{}' in gRPC service URL '{}'; "
                        + "using the configured transport instead", scheme, rawUrl);
            }
        }

        // A gRPC target is host:port, so drop any path pasted along with it.
        int slash = url.indexOf('/');
        if (slash >= 0) {
            url = url.substring(0, slash).trim();
        }

        String host;
        int port;
        int colon = url.lastIndexOf(':');
        if (colon > 0) {
            host = url.substring(0, colon).trim();
            String portText = url.substring(colon + 1).trim();
            try {
                port = Integer.parseInt(portText);
            } catch (NumberFormatException e) {
                LOG.error("Invalid port '{}' in gRPC service URL '{}', defaulting to {}",
                        portText, rawUrl, DEFAULT_PORT);
                port = DEFAULT_PORT;
            }
        } else {
            host = url;
            port = DEFAULT_PORT;
        }
        if (host.isEmpty()) {
            host = DEFAULT_HOST;
        }
        return new GrpcServiceTarget(host, port, tls);
    }
}
