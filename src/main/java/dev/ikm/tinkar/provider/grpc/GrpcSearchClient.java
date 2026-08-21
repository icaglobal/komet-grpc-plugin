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

import dev.ikm.tinkar.schema.PublicId;
import dev.ikm.tinkar.service.proto.SearchSortOption;
import dev.ikm.tinkar.service.proto.TinkarConceptEntityResponse;
import dev.ikm.tinkar.service.proto.TinkarConceptIdRequest;
import dev.ikm.tinkar.service.proto.TinkarConceptSearchWithSortRequest;
import dev.ikm.tinkar.service.proto.TinkarConceptSearchWithSortResponse;
import dev.ikm.tinkar.service.proto.TinkarConceptSemanticsResponse;
import dev.ikm.tinkar.service.proto.TinkarSearchServiceGrpc;
import dev.ikm.tinkar.service.proto.TinkarSemanticInfoResponse;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Manages a gRPC channel to a running tinkar-core service and exposes
 * concept-search operations. Configured via system properties:
 * <ul>
 *   <li>{@code komet.grpc.host} – hostname (default: {@code localhost})</li>
 *   <li>{@code komet.grpc.port} – port number (default: {@code 9090})</li>
 *   <li>{@code komet.grpc.tls} – {@code true} to use TLS instead of plaintext</li>
 *   <li>{@code komet.grpc.tls.ca} – PEM certificate to trust; implies TLS</li>
 *   <li>{@code komet.grpc.tls.authority} – override hostname verification</li>
 * </ul>
 * Call {@link #initialize(String, int)} once at startup, then access via {@link #get()}.
 * See {@link GrpcTlsConfig} for what the TLS settings mean.
 */
public class GrpcSearchClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSearchClient.class);

    private static volatile GrpcSearchClient instance;

    private final ManagedChannel channel;
    private final TinkarSearchServiceGrpc.TinkarSearchServiceBlockingStub stub;

    /**
     * Registers gRPC's default name-resolver and load-balancer providers explicitly.
     *
     * <p>gRPC normally discovers these through {@link java.util.ServiceLoader}, which is inert
     * here: this jar is a NAMED module, and a named module resolves services from
     * {@code provides} clauses only — never from {@code META-INF/services}. A {@code provides}
     * clause is impossible because the implementations are merged in by maven-shade-plugin at
     * package time, after module-info is compiled ("service implementation must be defined in
     * the same module as the provides directive").
     *
     * <p>gRPC's own hard-coded fallback does not rescue it either: the registries look up
     * {@code io.grpc.internal.*} providers reflectively, and that path yields nothing in this
     * module layer. Registering the two providers directly makes channel construction
     * independent of discovery altogether.
     *
     * <p>Idempotent: {@code register} replaces an equal provider, and this runs once per client.
     */
    private static void registerDefaultProviders() {
        io.grpc.NameResolverRegistry.getDefaultRegistry()
                .register(new io.grpc.internal.DnsNameResolverProvider());
        io.grpc.LoadBalancerRegistry.getDefaultRegistry()
                .register(new io.grpc.internal.PickFirstLoadBalancerProvider());
    }

    private GrpcSearchClient(String host, int port, GrpcTlsConfig tls) {
        registerDefaultProviders();
        // NettyChannelBuilder directly, not ManagedChannelBuilder.forAddress(): the latter
        // resolves a transport through the ManagedChannelProvider SPI, which cannot work from
        // this jar. ServiceLoader is inert (named module, see above), and grpc's hard-coded
        // fallback in ManagedChannelRegistry looks for the UNSHADED name
        // io.grpc.netty.NettyChannelProvider while the shaded artifact supplies
        // io.grpc.netty.shaded.io.grpc.netty.NettyChannelProvider. Naming the builder skips
        // provider discovery entirely.
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(host, port);
        applyTransportSecurity(builder, tls);
        if (tls.authorityOverride() != null) {
            builder.overrideAuthority(tls.authorityOverride());
        }
        this.channel = builder.build();
        this.stub = TinkarSearchServiceGrpc.newBlockingStub(channel);
        LOG.info("gRPC client initialised → {}:{} [{}]", host, port, tls.describe());
    }

    /**
     * Selects plaintext or TLS on the builder.
     *
     * <p>The SslContext comes from the SHADED {@code GrpcSslContexts}, matching the shaded
     * {@code NettyChannelBuilder} it is handed to. Mixing the shaded builder with an unshaded
     * context fails at runtime with a confusing type error, because they are different classes
     * that merely share a simple name.
     *
     * <p>No SslProvider is named: gRPC prefers the bundled tcnative/boringssl native and falls
     * back to the JDK provider, and both are viable here — the JDK has supported ALPN, which
     * gRPC requires, since Java 9.
     */
    private static void applyTransportSecurity(NettyChannelBuilder builder, GrpcTlsConfig tls) {
        if (!tls.enabled()) {
            builder.usePlaintext();
            return;
        }
        if (tls.caCertPath() == null) {
            // Validate against the JDK default trust store — the public-CA case.
            builder.useTransportSecurity();
            return;
        }
        File ca = new File(tls.caCertPath());
        if (!ca.isFile()) {
            throw new IllegalStateException("gRPC TLS trust material not found: "
                    + ca.getAbsolutePath()
                    + " (set " + GrpcTlsConfig.CA_PROPERTY + " to a readable PEM certificate,"
                    + " or unset it to use the JDK trust store)");
        }
        try {
            SslContext sslContext = GrpcSslContexts.forClient().trustManager(ca).build();
            builder.sslContext(sslContext);
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to build the gRPC TLS context from " + ca.getAbsolutePath(), e);
        }
    }

    /**
     * Creates and registers the singleton client, taking transport security from the
     * {@code komet.grpc.tls*} system properties.
     *
     * @param host gRPC server hostname
     * @param port gRPC server port
     */
    public static void initialize(String host, int port) {
        initialize(host, port, GrpcTlsConfig.fromSystemProperties());
    }

    /**
     * Creates and registers the singleton client with explicit transport security. Used when
     * the caller already knows the transport — for example a service URL carrying a
     * {@code grpcs://} scheme — rather than inferring it from system properties.
     *
     * @param host gRPC server hostname
     * @param port gRPC server port
     * @param tls  transport security settings
     */
    public static void initialize(String host, int port, GrpcTlsConfig tls) {
        instance = new GrpcSearchClient(host, port, tls);
    }

    /** Returns {@code true} when the client has been initialised. */
    public static boolean isAvailable() {
        return instance != null;
    }

    /** Returns the singleton client, or {@code null} if not yet initialised. */
    public static GrpcSearchClient get() {
        return instance;
    }

    /**
     * Calls {@code TinkarSearchService.ConceptSearchWithSort} on the remote service.
     *
     * @param query      free-text search string
     * @param maxResults maximum number of results to return
     * @param sortBy     sort order for results
     * @return the response from the server
     */
    public TinkarConceptSearchWithSortResponse conceptSearchWithSort(
            String query, int maxResults, SearchSortOption sortBy) {

        TinkarConceptSearchWithSortRequest request = TinkarConceptSearchWithSortRequest.newBuilder()
                .setQuery(query)
                .setMaxResults(maxResults)
                .setSortBy(sortBy)
                .build();
        return stub.conceptSearchWithSort(request);
    }

    /**
     * Calls {@code TinkarSearchService.LoadConceptEntityGraph} on the remote service.
     * Returns the full entity graph (concept + semantics + patterns + stamps) so the
     * caller can load them into a local entity store and display concept details.
     *
     * @param publicId the concept's public ID (list of UUIDs)
     * @return the response with all related TinkarMsg entities, or an error response
     */
    public TinkarConceptEntityResponse loadConceptEntityGraph(PublicId publicId) {
        TinkarConceptIdRequest request = TinkarConceptIdRequest.newBuilder()
                .setPublicId(publicId)
                .build();
        return stub.loadConceptEntityGraph(request);
    }

    /**
     * Calls {@code TinkarSearchService.InspectConcept} on the remote service.
     * Returns every semantic attached to the concept, each with its pattern name and
     * named field values — the discovery counterpart to {@link #getSemanticInfo}, which
     * requires a semantic's UUID up front.
     *
     * @param publicId the concept's public ID (list of UUIDs)
     * @return the response listing the concept's semantics, or an error response
     */
    public TinkarConceptSemanticsResponse inspectConcept(PublicId publicId) {
        TinkarConceptIdRequest request = TinkarConceptIdRequest.newBuilder()
                .setPublicId(publicId)
                .build();
        return stub.inspectConcept(request);
    }

    /**
     * Calls {@code TinkarSearchService.GetSemanticInfo} on the remote service.
     * Returns the field values, pattern name, and STAMP info for a single semantic instance,
     * fetched by the semantic's own public ID — as opposed to {@link #conceptSearchWithSort}
     * or {@code InspectConcept}, which operate at concept granularity.
     *
     * @param publicId the semantic's public ID (list of UUIDs)
     * @return the response with the semantic's field-level detail, or an error response
     */
    public TinkarSemanticInfoResponse getSemanticInfo(PublicId publicId) {
        TinkarConceptIdRequest request = TinkarConceptIdRequest.newBuilder()
                .setPublicId(publicId)
                .build();
        return stub.getSemanticInfo(request);
    }

    /**
     * Calls {@code TinkarSearchService.GetEntityByPublicId} on the remote service.
     * Returns a single entity plus its version stamps — used as a cache-miss fallback
     * by {@link GrpcPrimitiveDataService}.
     *
     * @param publicId the entity's public ID (list of UUIDs)
     * @return the response with the entity and its stamps, or an error response
     */
    public TinkarConceptEntityResponse getEntityByPublicId(PublicId publicId) {
        TinkarConceptIdRequest request = TinkarConceptIdRequest.newBuilder()
                .setPublicId(publicId)
                .build();
        return stub.getEntityByPublicId(request);
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            LOG.info("gRPC channel shut down");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
