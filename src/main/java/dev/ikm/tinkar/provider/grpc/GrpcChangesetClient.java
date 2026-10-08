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

import com.google.protobuf.ByteString;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.service.EntityCountSummary;
import dev.ikm.tinkar.common.service.RemoteChangesetService.ProgressListener;
import dev.ikm.tinkar.common.service.TrackingCallable;
import dev.ikm.tinkar.service.proto.CancelJobRequest;
import dev.ikm.tinkar.service.proto.EntityCountSummaryProto;
import dev.ikm.tinkar.service.proto.ExportEntitiesRequest;
import dev.ikm.tinkar.service.proto.ExportType;
import dev.ikm.tinkar.service.proto.IkeAdminGrpc;
import dev.ikm.tinkar.service.proto.ImportChangesetChunk;
import dev.ikm.tinkar.service.proto.ImportChangesetStart;
import dev.ikm.tinkar.service.proto.JobEvent;
import dev.ikm.tinkar.service.proto.JobInfo;
import dev.ikm.tinkar.service.proto.JobKind;
import dev.ikm.tinkar.service.proto.JobResult;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Imports and exports changesets on the remote service, as server jobs.
 *
 * <p>Both queue behind any reasoner run or other import or export on the server, so a call may
 * wait before its job starts; the listener is told what it is waiting behind.
 */
public final class GrpcChangesetClient {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcChangesetClient.class);

    /** Upload chunk size: well under gRPC's default 4 MiB message limit. */
    private static final int CHUNK_BYTES = 1024 * 1024;

    /** How often a blocked call checks whether its tracker was cancelled. */
    private static final long POLL_MS = 250L;

    private GrpcChangesetClient() {
    }

    /**
     * Uploads {@code changeset} and waits for the server to import it.
     *
     * <p>Imports cannot be cancelled on the server. Cancelling {@code tracker} ends this call — before
     * the upload completes that means nothing is imported; after, the import carries on regardless.
     */
    public static EntityCountSummary importChangeset(File changeset, ProgressListener listener,
                                                     TrackingCallable<?> tracker) {
        IkeAdminGrpc.IkeAdminStub stub = IkeAdminGrpc.newStub(channel());
        CompletableFuture<JobResult> finished = new CompletableFuture<>();
        AtomicReference<ClientCallStreamObserver<ImportChangesetChunk>> upload = new AtomicReference<>();

        stub.importChangeset(new ClientResponseObserver<ImportChangesetChunk, JobEvent>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<ImportChangesetChunk> requestStream) {
                upload.set(requestStream);
            }

            @Override
            public void onNext(JobEvent event) {
                if (event.hasResult()) {
                    finished.complete(event.getResult());
                } else {
                    report(event, listener);
                }
            }

            @Override
            public void onError(Throwable t) {
                finished.completeExceptionally(t);
            }

            @Override
            public void onCompleted() {
                finished.completeExceptionally(new IllegalStateException(
                        "Import stream ended without a result — the server closed it early"));
            }
        });

        ClientCallStreamObserver<ImportChangesetChunk> requestStream = upload.get();
        LOG.info("Uploading {} ({} bytes) to import on the remote service", changeset.getName(), changeset.length());
        try (InputStream in = new BufferedInputStream(new FileInputStream(changeset))) {
            requestStream.onNext(ImportChangesetChunk.newBuilder()
                    .setStart(ImportChangesetStart.newBuilder().setFileName(changeset.getName()))
                    .build());
            byte[] buffer = new byte[CHUNK_BYTES];
            long sent = 0;
            int read;
            while ((read = in.readNBytes(buffer, 0, buffer.length)) > 0) {
                // Flow control: wait for the transport rather than queueing the whole file in memory.
                while (!requestStream.isReady() && !finished.isDone()) {
                    throwIfCancelled(tracker, requestStream, "Upload cancelled");
                    Thread.sleep(5);
                }
                if (finished.isDone()) {
                    break; // the server answered early — rejected the upload; its reason is in the result
                }
                throwIfCancelled(tracker, requestStream, "Upload cancelled");
                requestStream.onNext(ImportChangesetChunk.newBuilder()
                        .setData(ByteString.copyFrom(buffer, 0, read)).build());
                sent += read;
                if (listener != null) {
                    listener.onProgress(sent, changeset.length(), "Uploading " + changeset.getName());
                }
            }
            if (!finished.isDone()) {
                requestStream.onCompleted();
            }
        } catch (IOException e) {
            requestStream.cancel("Could not read the changeset", e);
            throw new IllegalStateException("Could not read " + changeset + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            requestStream.cancel("Interrupted", e);
            throw new CancellationException("Import upload interrupted");
        }

        JobResult result = await(finished, tracker, () -> requestStream.cancel("Stopped waiting", null),
                "Stopped waiting for the remote import — it carries on on the server");
        EntityCountSummary counts = outcome(result, "import");
        refreshLocalCopies(changeset);
        return counts;
    }

    /**
     * After a server import, replaces the local store's copies of the components the changeset
     * carried — the file was ours, so we know exactly which — with the server's merged ones.
     */
    private static void refreshLocalCopies(File changeset) {
        if (!(dev.ikm.tinkar.common.service.PrimitiveData.get() instanceof GrpcPrimitiveDataService store)) {
            return;
        }
        java.util.List<java.util.List<java.util.UUID>> components = new java.util.ArrayList<>();
        try (java.util.zip.ZipInputStream zip = new java.util.zip.ZipInputStream(
                new BufferedInputStream(new FileInputStream(changeset)))) {
            java.util.zip.ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                if (entry.getName().startsWith("META-INF/")) {
                    continue;
                }
                dev.ikm.tinkar.schema.TinkarMsg record;
                while ((record = dev.ikm.tinkar.schema.TinkarMsg.parseDelimitedFrom(zip)) != null) {
                    dev.ikm.tinkar.schema.PublicId id = switch (record.getValueCase()) {
                        case CONCEPT_CHRONOLOGY -> record.getConceptChronology().getPublicId();
                        case SEMANTIC_CHRONOLOGY -> record.getSemanticChronology().getPublicId();
                        case PATTERN_CHRONOLOGY -> record.getPatternChronology().getPublicId();
                        case STAMP_CHRONOLOGY -> record.getStampChronology().getPublicId();
                        case VALUE_NOT_SET -> null;
                    };
                    if (id != null) {
                        components.add(id.getUuidsList().stream().map(java.util.UUID::fromString).toList());
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            // The import itself succeeded; a stale view until restart is the worst of this.
            LOG.warn("Imported, but could not read {} to refresh local copies: {}", changeset, e.toString());
            return;
        }
        store.refreshFromServer(components);
    }

    /** Exports the entities changed between two times into {@code target}. */
    public static EntityCountSummary exportChangeSet(File target, long fromEpochMillis, long toEpochMillis,
                                                     ProgressListener listener, TrackingCallable<?> tracker) {
        return export(target, ExportEntitiesRequest.newBuilder()
                .setExportType(ExportType.TEMPORAL)
                .setFromEpochMillis(fromEpochMillis)
                .setToEpochMillis(toEpochMillis)
                .build(), listener, tracker);
    }

    /** Exports the members of {@code membershipTags} into {@code target}. */
    public static EntityCountSummary exportMembership(File target, List<PublicId> membershipTags,
                                                      ProgressListener listener, TrackingCallable<?> tracker) {
        ExportEntitiesRequest.Builder request = ExportEntitiesRequest.newBuilder().setExportType(ExportType.MEMBERSHIP);
        membershipTags.forEach(tag -> request.addMembershipTags(dev.ikm.tinkar.schema.PublicId.newBuilder()
                .addAllUuids(tag.asUuidList().stream().map(Object::toString).toList())));
        return export(target, request.build(), listener, tracker);
    }

    /**
     * Runs an export on the server and writes the file it sends into {@code target}.
     *
     * <p>Cancelling {@code tracker} sends CancelJob, then keeps reading until the server reports the
     * export stopped — so this returns only once the server has given up on it too.
     */
    private static EntityCountSummary export(File target, ExportEntitiesRequest request,
                                             ProgressListener listener, TrackingCallable<?> tracker) {
        IkeAdminGrpc.IkeAdminBlockingStub stub = IkeAdminGrpc.newBlockingStub(channel());
        AtomicReference<String> jobId = new AtomicReference<>();
        Thread watcher = startCancelWatcher(tracker, jobId, stub);
        JobResult result = null;
        try (OutputStream out = new BufferedOutputStream(new FileOutputStream(target))) {
            LOG.info("Requesting remote {} export into {}", request.getExportType(), target);
            Iterator<JobEvent> events = stub.exportEntities(request);
            while (events.hasNext()) {
                JobEvent event = events.next();
                switch (event.getEventCase()) {
                    case JOB -> jobId.set(event.getJob().getJobId());
                    case FILE_CHUNK -> event.getFileChunk().writeTo(out);
                    case RESULT -> result = event.getResult();
                    default -> report(event, listener);
                }
            }
        } catch (IOException e) {
            deleteQuietly(target);
            throw new IllegalStateException("Could not write " + target + ": " + e.getMessage(), e);
        } catch (StatusRuntimeException e) {
            deleteQuietly(target);
            throw new IllegalStateException("Remote export failed: " + e.getStatus().getDescription(), e);
        } finally {
            if (watcher != null && (tracker == null || !tracker.isCancelled())) {
                watcher.interrupt();
            }
        }
        if (result == null || !result.getSuccess()) {
            deleteQuietly(target);
        } else if (target.length() != result.getFileSizeBytes()) {
            deleteQuietly(target);
            throw new IllegalStateException("Remote export arrived incomplete: " + target.length()
                    + " of " + result.getFileSizeBytes() + " bytes");
        }
        return outcome(result, "export");
    }

    /** Sends CancelJob once {@code tracker} is cancelled and the job's id is known. */
    private static Thread startCancelWatcher(TrackingCallable<?> tracker, AtomicReference<String> jobId,
                                             IkeAdminGrpc.IkeAdminBlockingStub stub) {
        if (tracker == null) {
            return null;
        }
        Thread watcher = new Thread(() -> {
            try {
                while (!tracker.isCancelled() || jobId.get() == null) {
                    Thread.sleep(POLL_MS);
                }
            } catch (InterruptedException e) {
                return;
            }
            LOG.info("Cancel requested — asking the server to stop export job {}", jobId.get());
            try {
                stub.cancelJob(CancelJobRequest.newBuilder().setJobId(jobId.get()).build());
            } catch (StatusRuntimeException e) {
                // FAILED_PRECONDITION: it finished first. Its result is on the way regardless.
                LOG.info("Export cancel not applied: {}", e.getStatus());
            }
        }, "grpc-export-cancel-watcher");
        watcher.setDaemon(true);
        watcher.start();
        return watcher;
    }

    private static JobResult await(CompletableFuture<JobResult> finished, TrackingCallable<?> tracker,
                                   Runnable stop, String stoppedMessage) {
        try {
            while (true) {
                try {
                    return finished.get(POLL_MS, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    if (tracker != null && tracker.isCancelled()) {
                        stop.run();
                        throw new CancellationException(stoppedMessage);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop.run();
            throw new CancellationException(stoppedMessage);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            String why = cause instanceof StatusRuntimeException status
                    ? status.getStatus().getDescription() : cause.getMessage();
            throw new IllegalStateException("Remote import failed: " + why, cause);
        }
    }

    private static void throwIfCancelled(TrackingCallable<?> tracker,
                                         ClientCallStreamObserver<ImportChangesetChunk> requestStream, String why) {
        if (tracker != null && tracker.isCancelled()) {
            requestStream.cancel(why, null);
            throw new CancellationException(why + " — nothing was imported");
        }
    }

    private static void report(JobEvent event, ProgressListener listener) {
        if (listener == null) {
            return;
        }
        switch (event.getEventCase()) {
            case QUEUED -> {
                if (event.getQueued().getAheadCount() > 0) {
                    listener.onProgress(-1, 1, "Waiting behind " + event.getQueued().getAheadList().stream()
                            .map(GrpcChangesetClient::describe).collect(Collectors.joining(", ")));
                }
            }
            case STARTED_AT -> listener.onProgress(-1, 1, "Started on the server");
            case PROGRESS -> listener.onProgress(event.getProgress().getDone(), event.getProgress().getTotal(),
                    event.getProgress().getMessage());
            default -> {
            }
        }
    }

    private static String describe(JobInfo job) {
        String kind = job.getKind() == JobKind.REASONER ? "a reasoner run"
                : job.getKind() == JobKind.IMPORT ? "an import" : "an export";
        return kind + " (" + job.getState().name().toLowerCase() + ")";
    }

    private static EntityCountSummary outcome(JobResult result, String what) {
        if (result == null) {
            throw new IllegalStateException("Remote " + what + " ended without a result");
        }
        if (result.getCancelled()) {
            throw new CancellationException("Remote " + what + " was cancelled");
        }
        if (!result.getSuccess()) {
            throw new IllegalStateException("Remote " + what + " failed: " + result.getErrorMessage());
        }
        EntityCountSummaryProto counts = result.getEntityCounts();
        LOG.info("Remote {} finished in {}ms: {} concepts, {} semantics, {} patterns, {} stamps", what,
                result.getDurationMs(), counts.getConceptsCount(), counts.getSemanticsCount(),
                counts.getPatternsCount(), counts.getStampsCount());
        return new EntityCountSummary(counts.getConceptsCount(), counts.getSemanticsCount(),
                counts.getPatternsCount(), counts.getStampsCount());
    }

    private static io.grpc.Channel channel() {
        if (!GrpcSearchClient.isAvailable()) {
            throw new IllegalStateException("gRPC client not initialised — cannot reach the remote service");
        }
        return GrpcSearchClient.get().channel();
    }

    private static void deleteQuietly(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            LOG.warn("Could not delete {}: {}", file, e.toString());
        }
    }
}
