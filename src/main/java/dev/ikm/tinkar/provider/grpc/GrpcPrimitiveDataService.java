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

import java.util.function.BinaryOperator;
import java.util.function.ObjLongConsumer;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;

import dev.ikm.tinkar.common.id.Nid;
import dev.ikm.tinkar.common.service.SequentialNids;
import dev.ikm.tinkar.entity.changeset.SchemaIds;
import dev.ikm.tinkar.common.service.internal.EntityStore;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.impl.NidLayout;
import dev.ikm.tinkar.common.service.DataActivity;
import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.DataServiceProperty;
import dev.ikm.tinkar.common.service.DataUriOption;
import dev.ikm.tinkar.common.service.NidGenerator;
import dev.ikm.tinkar.common.service.NoLocalUserStore;
import dev.ikm.tinkar.common.service.PrimitiveDataSearchResult;
import dev.ikm.tinkar.common.service.PrimitiveDataService;
import dev.ikm.tinkar.common.service.ProviderController;
import dev.ikm.tinkar.common.service.ServiceExclusionGroup;
import dev.ikm.tinkar.common.service.ServiceLifecyclePhase;
import dev.ikm.tinkar.common.sets.ConcurrentHashSet;
import dev.ikm.tinkar.common.util.ints2long.IntsInLong;
import dev.ikm.tinkar.entity.EntityRecordFactory;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.ConceptEntity;
import dev.ikm.tinkar.entity.PatternEntity;
import dev.ikm.tinkar.entity.SemanticEntity;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.transform.TinkarSchemaToEntityTransformer;
import dev.ikm.tinkar.terms.EntityFacade;
import dev.ikm.tinkar.terms.KernelConcept;
import dev.ikm.tinkar.terms.KernelPattern;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ObjIntConsumer;

/**
 * Purpose-built {@link PrimitiveDataService} implementation for gRPC mode.
 *
 * <p>Replaces the in-memory store in gRPC deployments. Entities are loaded into this
 * in-memory store by {@link GrpcSearchService#loadConceptWithSemantics} (via the
 * {@code LoadConceptEntityGraph} batch call). If an entity is requested that was not
 * prefetched, {@link #getBytes} falls back to a single-entity gRPC call
 * ({@code GetEntityByPublicId}) using the reverse NID→UUID map maintained here.
 *
 * <p>Unlike the spined array's ephemeral mode, this class has no search-indexing side effects in
 * {@link #merge} — search in gRPC mode is handled entirely by {@link GrpcSearchService}.
 */
public class GrpcPrimitiveDataService implements PrimitiveDataService, EntityStore, NidGenerator, NoLocalUserStore {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcPrimitiveDataService.class);

    protected static volatile GrpcPrimitiveDataService singleton;
    protected static final LongAdder writeSequence = new LongAdder();

    // Primary entity byte store: NID → serialized entity bytes
    private final ConcurrentHashMap<Integer, byte[]> nidComponentMap = new ConcurrentHashMap<>();

    // Deduplicates concurrent gRPC fetches for the same NID (thundering-herd guard)
    private final ConcurrentHashMap<Integer, CompletableFuture<byte[]>> inFlightFetches = new ConcurrentHashMap<>();

    // NIDs for which the server returned no entity — skip future gRPC retries (negative cache)
    private final ConcurrentHashSet<Integer> notFoundNids = new ConcurrentHashSet<>();

    // NIDs whose descriptions and navigation semantics have been asked of the server
    private final ConcurrentHashSet<Integer> semanticsFetched = new ConcurrentHashSet<>();

    // UUID ↔ NID bidirectional mapping
    private final ConcurrentHashMap<UUID, Integer> uuidNidMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, List<UUID>> nidToUuidsMap = new ConcurrentHashMap<>();

    // Secondary indices (mirrors the spined array's)
    private final ConcurrentHashMap<Integer, Integer> nidToPatternNidMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, long[]> nidToCitingComponentsNidMap = new ConcurrentHashMap<>();
    final ConcurrentHashSet<Integer> patternNids  = new ConcurrentHashSet<>();
    final ConcurrentHashSet<Integer> conceptNids  = new ConcurrentHashSet<>();
    final ConcurrentHashSet<Integer> semanticNids = new ConcurrentHashSet<>();
    final ConcurrentHashSet<Integer> stampNids    = new ConcurrentHashSet<>();

    private final AtomicInteger nextNid = new AtomicInteger(SequentialNids.FIRST_NID);

    private GrpcPrimitiveDataService() {
        LOG.info("Constructing GrpcPrimitiveDataService");
        NidLayout.activate(NidLayout.SEQUENTIAL);
    }

    public static GrpcPrimitiveDataService provider() {
        if (singleton == null) {
            synchronized (GrpcPrimitiveDataService.class) {
                if (singleton == null) {
                    singleton = new GrpcPrimitiveDataService();
                }
            }
        }
        return singleton;
    }

    // ── PrimitiveDataService ─────────────────────────────────────────

    @Override
    public long writeSequence() {
        return writeSequence.sum();
    }

    @Override
    public void close() {
        singleton = null;
    }

    @Override
    public long nidForUuids(UUID... uuids) {
        int nid = SequentialNids.nidForUuids(uuidNidMap, () -> Nid.narrowChecked(newNid()), uuids);
        nidToUuidsMap.putIfAbsent(nid, Arrays.asList(uuids));
        return nid;
    }

    @Override
    public long nidForUuids(ImmutableList<UUID> uuidList) {
        int nid = SequentialNids.nidForUuids(uuidNidMap, () -> Nid.narrowChecked(newNid()), uuidList);
        nidToUuidsMap.putIfAbsent(nid, uuidList.toList());
        return nid;
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        return uuidNidMap.containsKey(uuid);
    }

    @Override
    public boolean hasPublicId(PublicId publicId) {
        return publicId.asUuidList().stream().anyMatch(uuidNidMap::containsKey);
    }

    @Override
    public void forEach(ObjLongConsumer<byte[]> action) {
        nidComponentMap.forEach((nid, bytes) -> action.accept(bytes, nid));
    }

    @Override
    public void forEachParallel(ObjLongConsumer<byte[]> action) {
        nidComponentMap.entrySet().parallelStream()
                .forEach(e -> action.accept(e.getValue(), e.getKey()));
    }

    /**
     * Visits the entity bytes of each nid, in parallel, reading each through {@link #getBytes},
     * so an entity missing from the session store is fetched from the server; a nid with no
     * entity anywhere is passed over (IKE-Network/ike-issues#1250).
     */
    @Override
    public void forEachParallel(ImmutableLongList nids, ObjLongConsumer<byte[]> action) {
        nids.primitiveParallelStream().forEach(nid -> acceptBytes(Nid.narrowChecked(nid), action));
    }

    /**
     * Visits the entity bytes of each nid, in order, reading each through {@link #getBytes}, so
     * an entity missing from the session store is fetched from the server; a nid with no entity
     * anywhere is passed over (IKE-Network/ike-issues#1250).
     */
    @Override
    public void forEach(ImmutableLongList nids, ObjLongConsumer<byte[]> action) {
        nids.forEach(nid -> acceptBytes(Nid.narrowChecked(nid), action));
    }

    private void acceptBytes(int nid, ObjLongConsumer<byte[]> action) {
        byte[] bytes = getBytes(nid);
        if (bytes != null) {
            action.accept(bytes, nid);
        }
    }

    /**
     * Returns serialized entity bytes for {@code nid}.
     *
     * <p>Checks the local store first. On a cache miss, looks up the UUID(s) for this NID
     * in the reverse map and issues a {@code GetEntityByPublicId} gRPC call to fetch and
     * cache the entity before returning.
     *
     * <p>Concurrent requests for the same NID are deduplicated: only one thread makes the
     * gRPC call while others wait on a shared {@link CompletableFuture} (thundering-herd guard).
     *
     * <p>The transformer callbacks write directly into {@code nidComponentMap} via
     * {@link #merge} rather than going through {@code EntityService.putEntity()}, which
     * avoids a Caffeine re-entrance problem: {@code EntityProvider.getEntityFast} calls
     * {@code getBytes} from inside a Caffeine {@code get(key, loader)} computation, and
     * a {@code cache.put()} for the same key from within that loader can be silently
     * dropped, leaving {@code nidComponentMap} empty and causing an infinite gRPC retry loop.
     */
    /**
     * Replaces this store's copies of {@code components} with the server's current ones.
     *
     * <p>For after a change made on the server rather than through this client — a changeset import.
     * This store fetches an entity only when it does not hold it, so a component it already holds
     * would otherwise keep showing its old versions until Komet restarts. Components it has never
     * held are skipped: they will be fetched, current, when first needed. Ones it looked for and did
     * not find are forgotten as not-found, since the import may have just created them.
     *
     * <p>Goes through the entity service, so caches are invalidated and open views are told.
     *
     * @return how many components were refreshed
     */
    public int refreshFromServer(Iterable<List<UUID>> components) {
        if (!GrpcSearchClient.isAvailable()) {
            return 0;
        }
        // An import may have brought new patterns and members: ask again when next needed.
        patternsListed = false;
        membersAsked.clear();
        TinkarSchemaToEntityTransformer transformer = TinkarSchemaToEntityTransformer.getInstance();
        int refreshed = 0;
        for (List<UUID> uuids : components) {
            Integer nid = null;
            for (UUID uuid : uuids) {
                nid = uuidNidMap.get(uuid);
                if (nid != null) {
                    break;
                }
            }
            if (nid == null) {
                continue;
            }
            notFoundNids.remove(nid);
            if (!nidComponentMap.containsKey(nid)) {
                continue;
            }
            try {
                var response = GrpcSearchClient.get().getEntityByPublicId(dev.ikm.tinkar.schema.PublicId.newBuilder()
                        .addAllUuids(uuids.stream().map(UUID::toString).toList()).build());
                if (!response.getSuccess()) {
                    continue;
                }
                // A server says nothing of how its vertex identities were minted, so they are
                // correlated as they arrive, not kept (IKE-Network/ike-issues#1310).
                for (dev.ikm.tinkar.schema.TinkarMsg msg : response.getEntitiesList()) {
                    transformer.transform(msg,
                            entity -> EntityService.get().putEntity(entity, DataActivity.LOADING_UNDERIVED_CHANGE_SET),
                            stamp -> EntityService.get().putEntity(stamp, DataActivity.LOADING_UNDERIVED_CHANGE_SET));
                }
                refreshed++;
            } catch (RuntimeException e) {
                LOG.warn("Could not refresh {} from the server: {}", uuids, e.getMessage());
            }
        }
        LOG.info("Refreshed {} locally held component(s) from the server", refreshed);
        return refreshed;
    }

    @Override
    public byte[] getBytes(long nid) {
        byte[] bytes = nidComponentMap.get(Nid.narrowChecked(nid));
        if (bytes != null) {
            return bytes;
        }

        if (notFoundNids.contains(Nid.narrowChecked(nid))) {
            return null;
        }

        List<UUID> uuids = nidToUuidsMap.get(Nid.narrowChecked(nid));
        if (uuids == null || uuids.isEmpty() || !GrpcSearchClient.isAvailable()) {
            return null;
        }

        CompletableFuture<byte[]> myFuture = new CompletableFuture<>();
        CompletableFuture<byte[]> existing = inFlightFetches.putIfAbsent(Nid.narrowChecked(nid), myFuture);
        if (existing != null) {
            // Another thread is already fetching this NID — wait for its result
            try {
                return existing.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } catch (ExecutionException e) {
                LOG.warn("gRPC fallback failed (via in-flight wait) for nid {}: {}", nid, e.getCause().getMessage());
                return null;
            }
        }

        // Won the race — perform the gRPC call
        try {
            if (!fetchFromServer(Nid.narrowChecked(nid), uuids)) {
                LOG.debug("gRPC fallback: entity not found on server for nid {}", nid);
                notFoundNids.add(Nid.narrowChecked(nid));
                myFuture.complete(null);
                return null;
            }

            byte[] result = nidComponentMap.get(Nid.narrowChecked(nid));
            if (result == null) {
                LOG.warn("gRPC fallback: entity returned but not stored in nidComponentMap for nid {}", nid);
                notFoundNids.add(Nid.narrowChecked(nid));
            }
            myFuture.complete(result);
            return result;
        } catch (Exception e) {
            LOG.warn("gRPC fallback failed for nid {}: {}", nid, e.getMessage());
            myFuture.completeExceptionally(e);
            return null;
        } finally {
            inFlightFetches.remove(Nid.narrowChecked(nid));
        }
    }

    /**
     * Fetches the entity with the given UUIDs — with its descriptions, navigation semantics and
     * stamps — and stores all of it here.
     *
     * @return false when the server does not have it
     */
    private boolean fetchFromServer(int nid, List<UUID> uuids) {
        semanticsFetched.add(nid);
        dev.ikm.tinkar.schema.PublicId protoId = SchemaIds.toSchema(uuids.toArray(UUID[]::new));
        var response = GrpcSearchClient.get().getEntityByPublicId(protoId);
        if (!response.getSuccess() || response.getEntitiesList().isEmpty()) {
            return false;
        }

        store(response.getEntitiesList());
        return true;
    }

    /**
     * Stores entities the server sent. Bytes go straight into nidComponentMap via merge() to avoid
     * a Caffeine re-entrance issue: calling EntityService.putEntity() from within a Caffeine
     * get(key, loader) for the same key may silently drop the put.
     */
    private void store(List<dev.ikm.tinkar.schema.TinkarMsg> messages) {
        TinkarSchemaToEntityTransformer transformer = TinkarSchemaToEntityTransformer.getInstance();
        for (dev.ikm.tinkar.schema.TinkarMsg msg : messages) {
            transformer.transform(msg,
                    entity -> merge(entity.nid(),
                            entity instanceof SemanticEntity<?> se ? se.patternNid() : Integer.MAX_VALUE,
                            entity instanceof SemanticEntity<?> se ? se.referencedComponentNid() : Integer.MAX_VALUE,
                            entity.getBytes(), entity, DataActivity.SYNCHRONIZABLE_EDIT),
                    stamp  -> merge(stamp.nid(), Integer.MAX_VALUE, Integer.MAX_VALUE,
                            stamp.getBytes(), stamp, DataActivity.SYNCHRONIZABLE_EDIT));
        }
    }

    /**
     * Fetches a component known only by its UUIDs before its semantics are looked up, once.
     *
     * <p>A concept reached through another one — a child named in a navigation semantic, say —
     * arrives here as a nid and UUIDs and nothing else. Looking up its semantics finds none, so
     * the navigator would show it with no name and no children. Components this store already
     * holds are left alone: the ones loaded with their semantics have them, and semantics and
     * stamps are not looked up this way.
     */
    private void fetchSemanticsIfUnseen(int componentNid) {
        if (semanticsFetched.contains(componentNid)
                || semanticNids.contains(componentNid)
                || stampNids.contains(componentNid)
                || nidToCitingComponentsNidMap.containsKey(componentNid)
                || notFoundNids.contains(componentNid)
                || !GrpcSearchClient.isAvailable()) {
            return;
        }
        List<UUID> uuids = nidToUuidsMap.get(componentNid);
        if (uuids == null || uuids.isEmpty()) {
            return;
        }
        try {
            fetchFromServer(componentNid, uuids);
        } catch (Exception e) {
            LOG.warn("Failed to fetch the semantics of nid {}: {}", componentNid, e.getMessage());
        }
    }

    @Override
    public byte[] merge(long nid, long patternNid, long referencedComponentNid,
                        byte[] value, Object sourceObject, DataActivity activity) {
        return merge(nid, patternNid, referencedComponentNid, value, sourceObject, PrimitiveDataService::merge);
    }

    @Override
    public byte[] mergeReplacingVersion(long nid, long patternNid, long referencedComponentNid, long withdrawnStampNid,
                                        byte[] value, Object sourceObject, DataActivity activity) {
        // As in the spined array: the version leaves the cached record inside the same atomic
        // map merge that merges the value in, so no reader sees the record with neither.
        return merge(nid, patternNid, referencedComponentNid, value, sourceObject,
                (stored, incoming) -> PrimitiveDataService.merge(
                        EntityRecordFactory.withoutVersion(stored, withdrawnStampNid), incoming));
    }

    private byte[] merge(long nid, long patternNid, long referencedComponentNid, byte[] value, Object sourceObject,
                         BinaryOperator<byte[]> accumulator) {
        nidToPatternNidMap.computeIfAbsent(Nid.narrowChecked(nid), k -> {
            if (!Nid.isNotApplicable(patternNid)) {
                long citationLong = IntsInLong.ints2Long(Nid.narrowChecked(nid), Nid.narrowChecked(patternNid));
                nidToCitingComponentsNidMap.merge(Nid.narrowChecked(referencedComponentNid),
                        new long[]{citationLong}, PrimitiveDataService::mergeCitations);
            }
            return Nid.narrowChecked(patternNid);
        });

        if (sourceObject instanceof ConceptEntity concept) {
            conceptNids.add(Nid.narrowChecked(concept.nid()));
        } else if (sourceObject instanceof SemanticEntity semanticEntity) {
            semanticNids.add(Nid.narrowChecked(semanticEntity.nid()));
        } else if (sourceObject instanceof PatternEntity patternEntity) {
            patternNids.add(Nid.narrowChecked(patternEntity.nid()));
        } else if (sourceObject instanceof StampEntity stampEntity) {
            stampNids.add(Nid.narrowChecked(stampEntity.nid()));
        }

        byte[] mergedBytes = nidComponentMap.merge(Nid.narrowChecked(nid), value, accumulator);
        writeSequence.increment();
        return mergedBytes;
    }

    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) {
        // This store has no index of its own: the server searches, and each matching semantic is
        // returned by nid, its entity fetched from the server when it is first read.
        if (!GrpcSearchClient.isAvailable() || query == null || query.isBlank()) {
            return new PrimitiveDataSearchResult[0];
        }
        var response = GrpcSearchClient.get().conceptSearchWithSort(query, maxResultSize,
                dev.ikm.tinkar.service.proto.SearchSortOption.TOP_COMPONENT);
        java.util.List<PrimitiveDataSearchResult> results = new java.util.ArrayList<>();
        for (var group : response.getGroupedResultsList()) {
            for (var match : group.getMatchingSemanticsList()) {
                if (!SchemaIds.hasUuids(match.getPublicId())) {
                    continue;
                }
                long nid = nidForUuids(SchemaIds.uuids(match.getPublicId()));
                results.add(new PrimitiveDataSearchResult(nid, match.getFieldIndex(), match.getScore(),
                        match.getHighlightedText()));
            }
        }
        return results.toArray(PrimitiveDataSearchResult[]::new);
    }

    @Override
    public String highlight(String query, String text) {
        // The server's highlighting is not available for arbitrary text; bold each query word here.
        if (query == null || query.isBlank() || text == null) {
            return text;
        }
        String highlighted = text;
        for (String word : query.strip().split("\\s+")) {
            String term = word.replaceAll("[^\\p{L}\\p{N}]", "");
            if (!term.isEmpty()) {
                highlighted = highlighted.replaceAll("(?i)(" + java.util.regex.Pattern.quote(term) + ")", "<B>$1</B>");
            }
        }
        return highlighted;
    }

    @Override
    public CompletableFuture<Void> recreateLuceneIndex() {
        throw new UnsupportedOperationException("No local Lucene index in gRPC mode");
    }

    @Override
    public void forEachSemanticNidOfPattern(long patternNid, LongProcedure procedure) {
        nidToPatternNidMap.forEach((nid, storedPatternNid) -> {
            if (patternNid == storedPatternNid) {
                procedure.accept(nid);
            }
        });
    }

    @Override
    public void forEachPatternNid(LongProcedure procedure) {
        listPatternsFromServer();
        patternNids.forEach(procedure::accept);
    }

    /** Whether every pattern of the server's store has been loaded here. */
    private volatile boolean patternsListed;

    /** The patterns the server listed; any other pattern held here was made here, a window's say. */
    private final ConcurrentHashSet<Integer> serverPatternNids = new ConcurrentHashSet<>();

    /**
     * Loads every pattern of the server's store, once: this store holds only what it has fetched,
     * so listing patterns from it alone shows a handful. Reset by {@link #refreshFromServer}, after
     * an import may have added patterns.
     */
    private void listPatternsFromServer() {
        if (patternsListed || !GrpcSearchClient.isAvailable()) {
            return;
        }
        synchronized (this) {
            if (patternsListed) {
                return;
            }
            try {
                var response = GrpcSearchClient.get().listPatterns();
                if (response.getSuccess()) {
                    store(response.getEntitiesList());
                    for (dev.ikm.tinkar.schema.TinkarMsg msg : response.getEntitiesList()) {
                        if (msg.hasPatternChronology()) {
                            serverPatternNids.add(Nid.narrowChecked(nidForUuids(
                                    SchemaIds.toPublicId(msg.getPatternChronology().getPublicId()).asUuidArray())));
                        }
                    }
                    patternsListed = true;
                    LOG.info("Loaded {} patterns from the server", response.getPatternCount());
                } else {
                    LOG.warn("Could not list the server's patterns: {}", response.getErrorMessage());
                }
            } catch (RuntimeException e) {
                LOG.warn("Could not list the server's patterns: {}", e.getMessage());
            }
        }
    }

    /** Most members one ListPatternMembers call returns; larger pages take several calls. */
    private static final int MEMBERS_PER_CALL = 1000;

    /**
     * The public id to ask the server for a pattern's members by, or null when the server does
     * not hold the pattern: one made here, whose members are only here.
     */
    private dev.ikm.tinkar.schema.PublicId serverPatternId(long patternNid) {
        if (!GrpcSearchClient.isAvailable()) {
            return null;
        }
        listPatternsFromServer();
        if (patternsListed && !serverPatternNids.contains(Nid.narrowChecked(patternNid))) {
            return null;
        }
        return schemaIdOf(patternNid);
    }

    @Override
    public int countSemanticsOfPattern(long patternNid) {
        dev.ikm.tinkar.schema.PublicId pattern = serverPatternId(patternNid);
        if (pattern == null) {
            return EntityStore.super.countSemanticsOfPattern(patternNid);
        }
        try {
            var response = GrpcSearchClient.get().listPatternMembers(pattern, 0, 1, null);
            if (response.getSuccess()) {
                store(response.getEntitiesList());
                return (int) Math.min(Integer.MAX_VALUE, response.getTotalCount());
            }
            LOG.warn("Could not count the members of pattern nid {}: {}", patternNid, response.getErrorMessage());
        } catch (RuntimeException e) {
            LOG.warn("Could not count the members of pattern nid {}: {}", patternNid, e.getMessage());
        }
        return EntityStore.super.countSemanticsOfPattern(patternNid);
    }

    @Override
    public long[] semanticNidsOfPattern(long patternNid, int offset, int limit) {
        dev.ikm.tinkar.schema.PublicId pattern = serverPatternId(patternNid);
        if (pattern == null) {
            return EntityStore.super.semanticNidsOfPattern(patternNid, offset, limit);
        }
        return membersFromServer(patternNid, pattern, offset, limit, null);
    }

    /**
     * Reads up to {@code limit} of a pattern's members from the server, a call per
     * {@value #MEMBERS_PER_CALL}, storing what each page brings: the members, their stamps, and
     * their referenced components with descriptions, so they display without a call per row.
     */
    private long[] membersFromServer(long patternNid, dev.ikm.tinkar.schema.PublicId pattern,
                                     int offset, int limit, String text) {
        // The JDK's builder: this module reads Eclipse Collections' API, not its implementation.
        java.util.stream.LongStream.Builder nids = java.util.stream.LongStream.builder();
        int found = 0;
        int next = Math.max(0, offset);
        try {
            while (found < limit) {
                int want = Math.min(MEMBERS_PER_CALL, limit - found);
                var response = GrpcSearchClient.get().listPatternMembers(pattern, next, want, text);
                if (!response.getSuccess()) {
                    LOG.warn("Could not list the members of pattern nid {}: {}", patternNid, response.getErrorMessage());
                    break;
                }
                store(response.getEntitiesList());
                for (dev.ikm.tinkar.schema.PublicId member : response.getMemberIdsList()) {
                    nids.add(nidForUuids(SchemaIds.toPublicId(member).asUuidArray()));
                    found++;
                }
                next += response.getMemberIdsCount();
                if (!response.getHasMore() || response.getMemberIdsCount() == 0) {
                    break;
                }
            }
        } catch (RuntimeException e) {
            LOG.warn("Could not list the members of pattern nid {}: {}", patternNid, e.getMessage());
        }
        return nids.build().toArray();
    }

    /** The public id this store knows {@code nid} by, as the schema writes it, or null if none. */
    private dev.ikm.tinkar.schema.PublicId schemaIdOf(long nid) {
        List<UUID> uuids = nidToUuidsMap.get(Nid.narrowChecked(nid));
        return uuids == null || uuids.isEmpty() ? null : SchemaIds.toSchema(uuids.toArray(UUID[]::new));
    }

    @Override
    public void forEachConceptNid(LongProcedure procedure) {
        conceptNids.forEach(procedure::accept);
    }

    @Override
    public void forEachStampNid(LongProcedure procedure) {
        stampNids.forEach(procedure::accept);
    }

    @Override
    public void forEachSemanticNid(LongProcedure procedure) {
        semanticNids.forEach(procedure::accept);
    }

    @Override
    public void forEachSemanticNidForComponent(long componentNid, LongProcedure procedure) {
        fetchSemanticsIfUnseen(Nid.narrowChecked(componentNid));
        long[] citationLongs = nidToCitingComponentsNidMap.get(Nid.narrowChecked(componentNid));
        if (citationLongs != null) {
            for (long citationLong : citationLongs) {
                procedure.accept((int) (citationLong >> 32));
            }
        }
    }

    @Override
    public void forEachSemanticNidForComponentOfPattern(long componentNid, long patternNid,
                                                        LongProcedure procedure) {
        int component = Nid.narrowChecked(componentNid);
        fetchSemanticsIfUnseen(component);
        if (!citesOfPattern(component, patternNid, procedure)
                && fetchMembersForComponentIfUnseen(component, patternNid)) {
            citesOfPattern(component, patternNid, procedure);
        }
    }

    /** Gives {@code procedure} the semantics of the pattern held here that cite the component; whether any did. */
    private boolean citesOfPattern(int component, long patternNid, LongProcedure procedure) {
        long[] citationLongs = nidToCitingComponentsNidMap.get(component);
        boolean found = false;
        if (citationLongs != null) {
            for (long citationLong : citationLongs) {
                int citingNid        = (int) (citationLong >> 32);
                int citingPatternNid = (int)  citationLong;
                if (patternNid == citingPatternNid) {
                    procedure.accept(citingNid);
                    found = true;
                }
            }
        }
        return found;
    }

    /** Components whose every semantic has been loaded here, by {@link GrpcSearchService#loadConceptWithSemantics}. */
    private final ConcurrentHashSet<Integer> semanticsComplete = new ConcurrentHashSet<>();

    /** Component and pattern pairs whose members have been asked of the server, as one long each. */
    private final ConcurrentHashSet<Long> membersAsked = new ConcurrentHashSet<>();

    /**
     * Records that every semantic of a component is now held here, so a pattern it has no member
     * of is not asked of the server again.
     */
    public void markSemanticsComplete(long nid) {
        semanticsComplete.add(Nid.narrowChecked(nid));
    }

    /**
     * Asks the server, once, for a component's members of a pattern this store holds none of — a
     * concept fetched with its names alone, say, whose membership in a pattern is then looked up.
     * Not asked for a semantic or stamp: those are looked up for every displayed description (its
     * dialects, for one), and would each become a call. Nor for a component whose every semantic was
     * loaded: holding none of the pattern means it has none.
     *
     * @return whether the server sent any
     */
    private boolean fetchMembersForComponentIfUnseen(int component, long patternNid) {
        if (semanticsComplete.contains(component) || semanticNids.contains(component)
                || stampNids.contains(component) || !GrpcSearchClient.isAvailable()
                || !membersAsked.add(IntsInLong.ints2Long(component, Nid.narrowChecked(patternNid)))) {
            return false;
        }
        dev.ikm.tinkar.schema.PublicId pattern = serverPatternId(patternNid);
        dev.ikm.tinkar.schema.PublicId componentId = schemaIdOf(component);
        if (pattern == null || componentId == null) {
            return false;
        }
        try {
            var response = GrpcSearchClient.get().listPatternMembers(pattern, 0, MEMBERS_PER_CALL, null, componentId);
            if (response.getSuccess() && response.getMemberIdsCount() > 0) {
                store(response.getEntitiesList());
                return true;
            }
        } catch (RuntimeException e) {
            LOG.warn("Could not fetch the members of pattern nid {} for nid {}: {}", patternNid, component, e.getMessage());
        }
        return false;
    }

    @Override
    public String name() {
        return "gRPC Primitive Data";
    }

    // ── NidGenerator ────────────────────────────────────────────────

    @Override
    public long newNid() {
        return nextNid.getAndIncrement();
    }

    // ── Controller ──────────────────────────────────────────────────

    /**
     * Lifecycle controller for {@link GrpcPrimitiveDataService}.
     * Registered via JPMS {@code provides} in {@code module-info.java} so that
     * {@code PrimitiveData.getControllerOptions()} discovers it alongside the standard
     * providers. Komet's {@code startGrpcMode()} selects it by name.
     */
    public static class Controller extends ProviderController<GrpcPrimitiveDataService>
            implements DataServiceController<PrimitiveDataService> {

        public static final String CONTROLLER_NAME = "Connect to gRPC service";

        public static final DataServiceProperty SERVICE_URL_PROPERTY =
                new DataServiceProperty("Service URL", false, true);

        /**
         * The well-known concepts every {@code Coordinates.*} default factory (Logic, Language,
         * Path/Stamp) resolves eagerly via {@code KernelConcept.X.nid()}. In gRPC mode the local
         * entity store starts empty, so any of these can be read before its on-demand fetch
         * completes — prefetching them all once, here, closes that gap for the whole class of
         * "well-known concept not yet locally resolvable" crashes (e.g. ike-issues#851) rather
         * than patching each call site as it's discovered.
         */
        private static final List<EntityFacade> BOOTSTRAP_CONCEPTS = List.of(
                KernelConcept.DEFINITION_DESCRIPTION_TYPE,
                KernelPattern.DESCRIPTION_PATTERN,
                KernelConcept.DEVELOPMENT_PATH,
                KernelPattern.EL_PLUS_PLUS_INFERRED_AXIOMS_PATTERN,
                KernelConcept.EL_PLUS_PLUS_PROFILE,
                KernelPattern.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN,
                KernelConcept.ENGLISH_LANGUAGE,
                KernelConcept.FULLY_QUALIFIED_NAME_DESCRIPTION_TYPE,
                KernelPattern.GB_DIALECT_PATTERN,
                KernelPattern.INFERRED_NAVIGATION_PATTERN,
                KernelConcept.LANGUAGE,
                KernelConcept.MASTER_PATH,
                KernelConcept.NAVIGATION_VERTEX,
                KernelConcept.PRIMORDIAL_PATH,
                KernelConcept.REGULAR_NAME_DESCRIPTION_TYPE,
                KernelConcept.SANDBOX_PATH,
                KernelConcept.SNOROCKET_CLASSIFIER,
                KernelPattern.SOLOR_CONCEPT_ASSEMBLAGE,
                KernelConcept.SOLOR_MODULE,
                KernelConcept.SOLOR_OVERLAY_MODULE,
                KernelConcept.SPANISH_LANGUAGE,
                KernelPattern.STATED_NAVIGATION_PATTERN,
                KernelConcept.USER,
                KernelPattern.US_DIALECT_PATTERN
        );

        private final Map<DataServiceProperty, String> properties = new LinkedHashMap<>();

        public Controller() {
            properties.put(SERVICE_URL_PROPERTY, "localhost:9095");
        }

        /**
         * Prefetches {@link #BOOTSTRAP_CONCEPTS} so the handful of well-known concepts backing
         * the default view coordinate are already resolvable by the time any window, card, or
         * menu is built. Each concept is fetched independently so one failure (e.g. a dataset
         * missing an optional concept) doesn't block the rest or fail gRPC-mode startup.
         */
        private static void prefetchBootstrapConcepts() {
            for (EntityFacade concept : BOOTSTRAP_CONCEPTS) {
                try {
                    GrpcSearchService.get().loadConceptWithSemantics(concept.publicId().asUuidList().toList());
                } catch (Exception e) {
                    LOG.warn("Failed to prefetch bootstrap concept {}: {}", concept, e.getMessage());
                }
            }
        }

        @Override
        protected GrpcPrimitiveDataService createProvider() {
            return GrpcPrimitiveDataService.provider();
        }

        @Override
        protected void startProvider(GrpcPrimitiveDataService provider) {
            if (!GrpcSearchService.isActive()) {
                GrpcTlsConfig tls = GrpcTlsConfig.fromSystemProperties();
                // -Dkomet.grpc.host / -Dkomet.grpc.port override the Service URL property,
                // which itself defaults to localhost:9095. This lets a caller auto-select
                // this controller by name (e.g. -Dkomet.datastore.controller) and still
                // point it at a non-default host/port without a UI round trip.
                String host = System.getProperty("komet.grpc.host");
                String portProp = System.getProperty("komet.grpc.port");
                int port;
                if (host != null || portProp != null) {
                    host = (host == null || host.isBlank()) ? "localhost" : host.trim();
                    try {
                        port = portProp == null || portProp.isBlank() ? 9095 : Integer.parseInt(portProp.trim());
                    } catch (NumberFormatException e) {
                        LOG.error("Invalid komet.grpc.port value '{}', defaulting to 9095", portProp);
                        port = 9095;
                    }
                } else {
                    GrpcServiceTarget target = GrpcServiceTarget.parse(
                            properties.getOrDefault(SERVICE_URL_PROPERTY, "localhost:9095"), tls);
                    host = target.host();
                    port = target.port();
                    tls = target.tls();
                }
                LOG.info("Initializing gRPC connection to {}:{} [{}]", host, port, tls.describe());
                GrpcSearchService.initialize(host, port, tls);
                prefetchBootstrapConcepts();
            }
        }


        @Override
        protected void stopProvider(GrpcPrimitiveDataService provider) {
            LOG.info("Stopping GrpcPrimitiveDataService");
            provider.close();
            if (GrpcSearchClient.isAvailable()) {
                GrpcSearchClient.get().close();
            }
        }

        @Override
        protected String getProviderName() {
            return "GrpcPrimitiveDataService";
        }

        @Override
        public ImmutableList<Class<?>> serviceClasses() {
            return Lists.immutable.of(PrimitiveDataService.class);
        }

        /**
         * Exposes the {@link GrpcSearchService} singleton as a {@code RemoteConceptSearchService}
         * for {@code ServiceLifecycleManager.getRunningService(RemoteConceptSearchService.class)}.
         * Not declared via {@link #serviceClasses()} because {@link #provider()} returns the
         * {@link GrpcPrimitiveDataService} instance, not the separate {@link GrpcSearchService}
         * singleton — this overrides the generic {@link ServiceLifecycle#getService()} fallback
         * lookup instead.
         */
        @Override
        public Optional<Object> getService() {
            return GrpcSearchService.isActive() ? Optional.of(GrpcSearchService.get()) : Optional.empty();
        }

        @Override
        public ServiceLifecyclePhase getLifecyclePhase() {
            return ServiceLifecyclePhase.DATA_STORAGE;
        }

        @Override
        public int getSubPriority() {
            return 40;
        }

        @Override
        public Optional<ServiceExclusionGroup> getMutualExclusionGroup() {
            return Optional.of(ServiceExclusionGroup.DATA_PROVIDER);
        }

        @Override
        public String controllerName() {
            return CONTROLLER_NAME;
        }

        @Override
        public ImmutableMap<DataServiceProperty, String> providerProperties() {
            return Maps.immutable.ofAll(properties);
        }

        @Override
        public void setDataServiceProperty(DataServiceProperty key, String value) {
            properties.put(key, value);
        }

        @Override
        public boolean running() {
            return GrpcPrimitiveDataService.singleton != null;
        }

        @Override
        public void start() {
            startup();
        }

        @Override
        public void stop() {
            shutdown();
        }

        @Override
        public void save() {
            // nothing to persist in gRPC read-only mode
        }

        @Override
        public void reload() {
            throw new UnsupportedOperationException("Cannot reload gRPC data service");
        }

        @Override
        public boolean isValidDataLocation(String name) {
            return false;
        }

        @Override
        public List<DataUriOption> providerOptions() {
            return List.of();
        }
    }
}
