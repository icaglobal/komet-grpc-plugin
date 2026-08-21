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

/**
 * Transport security settings for the gRPC channel.
 *
 * <p>Three states, in increasing specificity:
 * <ul>
 *   <li>{@link #disabled()} — plaintext, the default and the behaviour before TLS support.</li>
 *   <li>{@code enabled} with no CA — TLS validated against the JDK default trust store, which
 *       is what a certificate from a public CA needs. No trust material to distribute.</li>
 *   <li>{@code enabled} with {@code caCertPath} — TLS validated against that certificate,
 *       for a self-signed or privately-issued certificate. A self-signed certificate is its
 *       own trust anchor, so this is the same file the server presents.</li>
 * </ul>
 *
 * <p>{@code authorityOverride} sets the name used for hostname verification, independent of
 * the address dialled. It exists for the case where a certificate's {@code subjectAltName}
 * does not cover the host being connected to — a port-forward, an IP-addressed service, a
 * tunnel. It weakens the guarantee that the peer is who the address says, so it is a
 * development convenience, not something to set in a deployment.
 *
 * @param enabled           whether to use TLS rather than plaintext
 * @param caCertPath        PEM certificate to trust, or {@code null} for the JDK trust store
 * @param authorityOverride name to verify the certificate against, or {@code null} to use the
 *                          dialled host
 */
public record GrpcTlsConfig(boolean enabled, String caCertPath, String authorityOverride) {

    /** {@code komet.grpc.tls} — {@code true} to use TLS instead of plaintext. */
    public static final String TLS_PROPERTY = "komet.grpc.tls";

    /** {@code komet.grpc.tls.ca} — path to a PEM certificate to trust. */
    public static final String CA_PROPERTY = "komet.grpc.tls.ca";

    /** {@code komet.grpc.tls.authority} — override the name used for hostname verification. */
    public static final String AUTHORITY_PROPERTY = "komet.grpc.tls.authority";

    /** Plaintext: the default, and the behaviour before TLS support existed. */
    public static GrpcTlsConfig disabled() {
        return new GrpcTlsConfig(false, null, null);
    }

    /**
     * Reads the {@code komet.grpc.tls*} system properties.
     *
     * <p>Setting a CA path implies TLS: a caller who names trust material has already said
     * which transport they mean, and requiring a second flag alongside it only creates a way
     * to configure a CA that is silently ignored.
     */
    public static GrpcTlsConfig fromSystemProperties() {
        String ca = trimmedOrNull(System.getProperty(CA_PROPERTY));
        String authority = trimmedOrNull(System.getProperty(AUTHORITY_PROPERTY));
        boolean enabled = Boolean.parseBoolean(System.getProperty(TLS_PROPERTY, "false"))
                || ca != null;
        return new GrpcTlsConfig(enabled, ca, authority);
    }

    /**
     * Returns this config with TLS forced on — used when the service URL carries an explicit
     * {@code grpcs://} scheme, which is a stronger signal than the property default. Any CA
     * and authority already configured are preserved.
     */
    public GrpcTlsConfig withTlsEnabled() {
        return enabled ? this : new GrpcTlsConfig(true, caCertPath, authorityOverride);
    }

    /** Short description for logging, with no secret material in it. */
    public String describe() {
        if (!enabled) {
            return "plaintext";
        }
        StringBuilder sb = new StringBuilder("TLS");
        sb.append(caCertPath == null ? " (JDK trust store)" : " (CA: " + caCertPath + ")");
        if (authorityOverride != null) {
            sb.append(", authority override: ").append(authorityOverride);
        }
        return sb.toString();
    }

    private static String trimmedOrNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
