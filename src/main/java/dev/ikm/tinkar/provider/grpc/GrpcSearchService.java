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

import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.ProviderController;
import dev.ikm.tinkar.common.service.RemoteConceptSearchService;
import dev.ikm.tinkar.common.service.ServiceExclusionGroup;
import dev.ikm.tinkar.common.service.ServiceLifecyclePhase;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.transform.TinkarSchemaToEntityTransformer;
import dev.ikm.tinkar.common.service.SearchService;
import dev.ikm.tinkar.schema.PublicId;
import dev.ikm.tinkar.service.proto.SearchSortOption;
import dev.ikm.tinkar.service.proto.TinkarConceptEntityResponse;
import dev.ikm.tinkar.service.proto.TinkarConceptSemanticInfo;
import dev.ikm.tinkar.service.proto.TinkarConceptSemanticsResponse;
import dev.ikm.tinkar.service.proto.TinkarSemanticInfoResponse;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link SearchService} and {@link RemoteConceptSearchService} that
 * delegates search calls to a remote tinkar-core gRPC service via {@link GrpcSearchClient}.
 *
 * <p>This is the plug-in point for gRPC-backed search. When the application is started
 * with {@code -Dkomet.grpc.port}, this service is activated via {@link #initialize}.
 * Downstream code discovers it as a {@link RemoteConceptSearchService} via
 * {@code ServiceLifecycleManager.get().getRunningService(RemoteConceptSearchService.class)}
 * and calls {@link #searchGrouped} or {@link #searchFlat} instead of the local Lucene path.
 *
 * <p>The {@link #search} method satisfies the {@link SearchService} contract but returns
 * an empty array — all meaningful results come through the typed methods that carry
 * grouped/semantic structure back from the service.
 */
public class GrpcSearchService implements SearchService, RemoteConceptSearchService {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcSearchService.class);

    private static volatile GrpcSearchService INSTANCE;

    private GrpcSearchService() {}

    /**
     * Activates gRPC search mode by initializing the underlying {@link GrpcSearchClient},
     * taking transport security from the {@code komet.grpc.tls*} system properties.
     * Must be called once at startup before any search calls.
     */
    public static void initialize(String host, int port) {
        initialize(host, port, GrpcTlsConfig.fromSystemProperties());
    }

    /**
     * Activates gRPC search mode with explicit transport security, for a caller that already
     * knows the transport — e.g. a service URL carrying a {@code grpcs://} scheme.
     */
    public static void initialize(String host, int port, GrpcTlsConfig tls) {
        GrpcSearchClient.initialize(host, port, tls);
        INSTANCE = new GrpcSearchService();
        LOG.info("GrpcSearchService initialized → {}:{} [{}]", host, port, tls.describe());
    }

    /**
     * Returns {@code true} when gRPC mode has been initialized and is ready.
     */
    public static boolean isActive() {
        return INSTANCE != null && GrpcSearchClient.isAvailable();
    }

    /**
     * Returns the active singleton, or throws if not initialized.
     */
    public static GrpcSearchService get() {
        if (INSTANCE == null) {
            throw new IllegalStateException("GrpcSearchService not initialized — pass -Dkomet.grpc.port to activate");
        }
        return INSTANCE;
    }

    /**
     * Performs a search returning grouped results (TOP_COMPONENT modes).
     */
    @Override
    public List<GroupedResult> searchGrouped(String query, int maxResults, SortOption sortOption) {
        SearchSortOption protoSort = toProtoSort(sortOption);
        var response = GrpcSearchClient.get().conceptSearchWithSort(query, maxResults, protoSort);
        return response.getGroupedResultsList().stream()
                .map(g -> new GroupedResult(
                        g.getPublicIdList(),
                        g.getFullyQualifiedName(),
                        g.getActive(),
                        g.getTopScore(),
                        g.getMatchingSemanticsList().stream()
                                .map(m -> new MatchingSemantic(
                                        m.getHighlightedText(), m.getPlainText(), m.getScore()))
                                .toList()))
                .toList();
    }

    /**
     * Performs a search returning flat semantic results (SEMANTIC modes).
     */
    @Override
    public List<SemanticResult> searchFlat(String query, int maxResults, SortOption sortOption) {
        SearchSortOption protoSort = toProtoSort(sortOption);
        var response = GrpcSearchClient.get().conceptSearchWithSort(query, maxResults, protoSort);
        return response.getResultsList().stream()
                .map(r -> new SemanticResult(
                        r.getPublicIdList(),
                        r.getFullyQualifiedName(),
                        r.getHighlightedText(),
                        r.getActive(),
                        r.getScore()))
                .toList();
    }

    /**
     * Fetches the full entity graph for a concept from the gRPC service and loads it into
     * the local entity store (ephemeral provider in gRPC mode).  After this call returns,
     * {@code Entity.get(nid)} will find the concept and all its semantics, patterns, and
     * stamps.
     *
     * <p>The server is expected to implement {@code LoadConceptEntityGraph} and return
     * the concept's ConceptChronology, SemanticChronologies, PatternChronologies, and
     * StampChronologies as a list of {@code TinkarMsg} objects.
     *
     * @param publicIds the concept's public UUIDs (from a search result)
     * @return the local NID assigned to the concept after loading
     * @throws IllegalStateException    if gRPC is not initialized
     * @throws StatusRuntimeException   if the server call fails (e.g. UNIMPLEMENTED)
     */
    @Override
    public int loadConceptWithSemantics(List<UUID> publicIds) {
        if (!isActive()) {
            throw new IllegalStateException("GrpcSearchService not initialized");
        }
        PublicId protoPublicId = PublicId.newBuilder()
                .addAllUuids(publicIds.stream().map(UUID::toString).toList())
                .build();
        TinkarConceptEntityResponse response =
                GrpcSearchClient.get().loadConceptEntityGraph(protoPublicId);
        if (!response.getSuccess()) {
            throw new RuntimeException("LoadConceptEntityGraph failed: " + response.getErrorMessage());
        }
        TinkarSchemaToEntityTransformer transformer = TinkarSchemaToEntityTransformer.getInstance();
        for (dev.ikm.tinkar.schema.TinkarMsg msg : response.getEntitiesList()) {
            transformer.transform(
                    msg,
                    entity -> EntityService.get().putEntity(entity),
                    stamp  -> EntityService.get().putEntity(stamp));
        }
        return EntityService.get().nidForUuids(publicIds.toArray(new UUID[0]));
    }

    /**
     * One semantic field: its name (the field's meaning concept, from the governing pattern's
     * field definitions) and its formatted value.
     */
    public record NamedField(String name, String value) {}

    /**
     * A single semantic instance — the result of {@link #semanticInfo} and the element type of
     * {@link #conceptSemantics}.
     *
     * @param patternName the semantic's governing pattern (e.g. "Test Performed Pattern")
     * @param semanticId  the semantic's own UUID, usable with {@link #semanticInfo}
     * @param fields      the field values, each paired with its field name
     */
    public record SemanticInfo(String patternName, String semanticId, List<NamedField> fields) {}

    /**
     * Fetches the field values of a single semantic instance from the gRPC service, by the
     * semantic's own public ID — not the concept it's attached to. Use this (rather than
     * {@link #searchGrouped}/{@link #loadConceptWithSemantics}) when a UUID identifies a
     * semantic/pattern instance directly, e.g. a Test Performed record or a comment.
     *
     * @param publicId the semantic's public UUID
     * @return the semantic's pattern name and named field values
     * @throws IllegalStateException if gRPC is not initialized
     * @throws RuntimeException      if the server reports failure (including "not found")
     */
    public SemanticInfo semanticInfo(UUID publicId) {
        if (!isActive()) {
            throw new IllegalStateException("GrpcSearchService not initialized");
        }
        PublicId protoPublicId = PublicId.newBuilder().addUuids(publicId.toString()).build();
        TinkarSemanticInfoResponse response = GrpcSearchClient.get().getSemanticInfo(protoPublicId);
        if (!response.getSuccess()) {
            throw new RuntimeException("GetSemanticInfo failed: " + response.getErrorMessage());
        }
        return toSemanticInfo(response.getSemantic());
    }

    /**
     * Lists every semantic attached to a concept, each with its pattern name, own UUID, and
     * named field values. This is the discovery step {@link #semanticInfo} lacks: it turns a
     * concept into the set of semantic instances hanging off it, so structured data (Test
     * Performed records, identifiers, comments, …) is reachable from a concept alone.
     *
     * @param conceptId     the concept's public UUID
     * @param patternFilter optional case-insensitive substring matched against the pattern
     *                      name; null or blank returns every semantic
     * @return the concept's semantics, filtered when a pattern filter is given
     * @throws IllegalStateException if gRPC is not initialized
     * @throws RuntimeException      if the server reports failure
     */
    public List<SemanticInfo> conceptSemantics(UUID conceptId, String patternFilter) {
        if (!isActive()) {
            throw new IllegalStateException("GrpcSearchService not initialized");
        }
        PublicId protoPublicId = PublicId.newBuilder().addUuids(conceptId.toString()).build();
        TinkarConceptSemanticsResponse response = GrpcSearchClient.get().inspectConcept(protoPublicId);
        if (!response.getSuccess()) {
            throw new RuntimeException("InspectConcept failed: " + response.getErrorMessage());
        }
        String filter = (patternFilter == null || patternFilter.isBlank())
                ? null : patternFilter.trim().toLowerCase();
        return response.getSemanticsList().stream()
                .filter(s -> filter == null || s.getPatternName().toLowerCase().contains(filter))
                .map(GrpcSearchService::toSemanticInfo)
                .toList();
    }

    /**
     * Adapts a proto semantic to {@link SemanticInfo}, preferring the server's named fields and
     * falling back to positional names when talking to a server that predates them.
     */
    private static SemanticInfo toSemanticInfo(TinkarConceptSemanticInfo semantic) {
        List<NamedField> fields;
        if (semantic.getNamedFieldsCount() > 0) {
            fields = semantic.getNamedFieldsList().stream()
                    .map(f -> new NamedField(f.getName(), f.getValue()))
                    .toList();
        } else {
            List<NamedField> positional = new java.util.ArrayList<>();
            List<dev.ikm.tinkar.schema.Field> raw = semantic.getFieldsList();
            for (int i = 0; i < raw.size(); i++) {
                positional.add(new NamedField("field " + i, raw.get(i).getStringValue()));
            }
            fields = List.copyOf(positional);
        }
        String semanticId = semantic.getSemanticPublicId().getUuidsCount() > 0
                ? semantic.getSemanticPublicId().getUuids(0) : "";
        return new SemanticInfo(semantic.getPatternName(), semanticId, fields);
    }

    // --- SearchService contract ---

    @Override
    public void index(Object object) {
        LOG.debug("GrpcSearchService.index() called — no-op in gRPC mode");
    }

    @Override
    public void commit() throws IOException {
        LOG.debug("GrpcSearchService.commit() called — no-op in gRPC mode");
    }

    /**
     * Satisfies the {@link SearchService} contract. NIDs are 0 since the local entity
     * store is ephemeral; callers that need rich display should use {@link #searchGrouped}
     * or {@link #searchFlat} directly.
     */
    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) {
        List<SemanticResult> flat = searchFlat(query, maxResultSize, SortOption.SEMANTIC);
        return flat.stream()
                .map(r -> new PrimitiveDataSearchResult(0, 0, r.score(), r.highlightedText()))
                .toArray(PrimitiveDataSearchResult[]::new);
    }

    @Override
    public String highlight(String query, String text) {
        LOG.debug("GrpcSearchService.highlight() called — returning text unchanged in gRPC mode");
        return text;
    }

    @Override
    public CompletableFuture<Void> recreateIndex() {
        LOG.debug("GrpcSearchService.recreateIndex() called — no-op in gRPC mode");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String name() {
        return "GrpcSearchService";
    }

    private static SearchSortOption toProtoSort(SortOption sortOption) {
        return switch (sortOption) {
            case TOP_COMPONENT -> SearchSortOption.TOP_COMPONENT;
            case TOP_COMPONENT_ALPHA -> SearchSortOption.TOP_COMPONENT_ALPHA;
            case SEMANTIC -> SearchSortOption.SEMANTIC;
            case SEMANTIC_ALPHA -> SearchSortOption.SEMANTIC_ALPHA;
        };
    }

    /**
     * Controller registering this service as the JVM's search engine when Komet is backed by a
     * remote gRPC datastore.
     *
     * <p>This controller and {@code SearchProvider.Controller} (local Lucene) are the two members
     * of {@link ServiceExclusionGroup#SEARCH_ENGINE}, so exactly one of them activates. Selecting
     * this one keeps the Lucene indexer from starting, which is what stops an empty index
     * directory being created for a store that is never queried locally.
     *
     * <p>Selection is by name, via the lifecycle manager's
     * {@code service.lifecycle.group.SEARCH_ENGINE} property, set by komet-desktop when the user
     * picks the gRPC datastore. The name the manager matches is {@code GrpcSearchService.Controller}
     * — {@code OuterClass.InnerClass}, not the simple class name.
     *
     * <p>This controller deliberately owns no connection lifecycle. The channel is created and
     * closed by {@code GrpcPrimitiveDataService.Controller}, which runs in
     * {@link ServiceLifecyclePhase#DATA_STORAGE} (200) — before {@link ServiceLifecyclePhase#INDEXING}
     * (400) — so the singleton is always initialized by the time {@link #createProvider()} runs.
     */
    public static class Controller extends ProviderController<GrpcSearchService> {

        /** Creates the controller. Invoked by {@code ServiceLoader} during service discovery. */
        public Controller() {}

        @Override
        protected GrpcSearchService createProvider() {
            // Initialized during DATA_STORAGE by GrpcPrimitiveDataService.Controller.startProvider().
            // If that did not happen, this controller was selected without the gRPC datastore being
            // active; get() throws with a message naming the missing property rather than yielding a
            // half-built service.
            return GrpcSearchService.get();
        }

        @Override
        protected void startProvider(GrpcSearchService provider) {
            // No-op: the gRPC channel is opened by GrpcPrimitiveDataService.Controller.
        }

        @Override
        protected void stopProvider(GrpcSearchService provider) {
            // No-op: the channel is closed by GrpcPrimitiveDataService.Controller.stopProvider().
            // Closing it here would tear down the transport the datastore is still using.
        }

        @Override
        protected String getProviderName() {
            return "GrpcSearchService";
        }

        @Override
        public ImmutableList<Class<?>> serviceClasses() {
            return Lists.immutable.of(SearchService.class, RemoteConceptSearchService.class);
        }

        @Override
        public ServiceLifecyclePhase getLifecyclePhase() {
            return ServiceLifecyclePhase.INDEXING;
        }

        @Override
        public Optional<ServiceExclusionGroup> getMutualExclusionGroup() {
            return Optional.of(ServiceExclusionGroup.SEARCH_ENGINE);
        }

        @Override
        public int getSubPriority() {
            // Deliberately weaker than SearchProvider.Controller's 10.
            //
            // effectivePriority is phase.getBaseValue() + subPriority, so matching 10 would tie
            // both controllers at INDEXING(400) + 10 = 410. When no selection is specified the
            // lifecycle manager breaks ties with min() over that value, which on a tie resolves
            // by discovery-map iteration order — i.e. arbitrarily. This controller would then be
            // picked in a purely local session and fail in createProvider(), because nothing
            // initialized the gRPC client.
            //
            // Losing the default keeps local Lucene the safe fallback for any launcher that does
            // not set service.lifecycle.group.SEARCH_ENGINE. This controller is selected by name,
            // never by priority.
            return 60;
        }
    }
}
