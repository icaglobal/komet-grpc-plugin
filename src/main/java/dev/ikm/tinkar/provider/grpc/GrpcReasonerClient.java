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

import dev.ikm.tinkar.service.proto.CancelReasonerRequest;
import dev.ikm.tinkar.service.proto.IkeAdminGrpc;
import dev.ikm.tinkar.service.proto.RunReasonerEvent;
import dev.ikm.tinkar.service.proto.RunReasonerRequest;
import dev.ikm.tinkar.service.proto.RunReasonerResult;
import dev.ikm.tinkar.common.service.TrackingCallable;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/**
 * Runs the reasoner on the remote service.
 *
 * <p>The classification happens server-side and its inferred results are written to the
 * server's store. A client in gRPC mode cannot run the reasoner itself: the
 * {@code ReasonerService} SPI is a stateful pipeline over a local entity store, and in gRPC
 * mode the local store is ephemeral.
 *
 * <p>The call is server-streaming. Each phase of the pipeline arrives as it completes, then a
 * single terminal result. The phases are numbered and worded to match Komet's local
 * {@code RunReasonerTaskBase}, so the same progress UI can be driven either way.
 */
public final class GrpcReasonerClient {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcReasonerClient.class);

    private GrpcReasonerClient() {}

    /** Notified as each phase completes, mirroring the local reasoner task's progress. */
    @FunctionalInterface
    public interface PhaseListener {
        /**
         * @param step       1-based phase that just completed
         * @param totalSteps phases in the pipeline
         * @param message    what the phase did, in Komet's own wording
         */
        void onPhase(int step, int totalSteps, String message);
    }

    /**
     * The outcome of a classification, with concepts as public IDs.
     *
     * <p>Nids are assigned per store, so the server's are meaningless here; the caller resolves
     * these against its own store. {@code classifiedConceptCount} is a count rather than a list
     * on purpose — the server does not send that set, because Komet's results panel only ever
     * displays its size, and sending it exceeded gRPC's message limit on a real dataset.
     *
     * @param classifiedConceptCount how many concepts were classified
     * @param conceptsWithInferredChanges concepts whose inferred axioms changed
     * @param conceptsWithNavigationChanges concepts whose navigation changed
     * @param orphans concepts left without a parent
     * @param equivalentSets equivalence classes, each a list of concepts
     * @param commitTime when the server committed the inferred results; the caller advances its
     *                   view to this so the new inferences are visible
     * @param stampCoordinateText server-side coordinate, pre-rendered for display
     * @param logicCoordinateText server-side coordinate, pre-rendered for display
     * @param editCoordinateText server-side coordinate, pre-rendered for display
     * @param durationMs how long the pipeline took on the server
     */
    public record ReasonerOutcome(
            int classifiedConceptCount,
            List<List<UUID>> conceptsWithInferredChanges,
            List<List<UUID>> conceptsWithNavigationChanges,
            List<List<UUID>> orphans,
            List<List<List<UUID>>> equivalentSets,
            long commitTime,
            String stampCoordinateText,
            String logicCoordinateText,
            String editCoordinateText,
            long durationMs) {
    }

    /**
     * Runs the reasoner on the server, reporting phases as they complete.
     *
     * <p>A failed classification is returned by the server as a result with {@code success}
     * false rather than as a stream error, so it surfaces here as an exception carrying the
     * server's message — the caller handles one failure shape, not two.
     *
     * @param onPhase notified per phase; may be null
     * @return the classification outcome
     * @throws IllegalStateException if the client is not initialised, or the server reported a
     *                               failed classification
     */
    public static ReasonerOutcome runReasoner(PhaseListener onPhase) {
        return runReasoner(onPhase, null);
    }

    /**
     * As {@link #runReasoner(PhaseListener)}, stopping the run if {@code tracker} is cancelled.
     *
     * <p>The run belongs to the server, not to this call. If one is already going, this joins
     * it — its completed phases are replayed first — rather than starting another. Ending the
     * call early, or closing Komet, only stops watching; the classification carries on and a
     * later call picks it up again.
     *
     * <p>Cancelling {@code tracker} is the one thing that stops the run: it sends
     * {@code CancelReasoner}, then keeps reading until the server reports the run stopped, so
     * this returns only once nothing more will be written.
     *
     * @param tracker cancelling it stops the run; may be null
     * @throws CancellationException if the run was cancelled
     */
    public static ReasonerOutcome runReasoner(PhaseListener onPhase, TrackingCallable<?> tracker) {
        if (!GrpcSearchClient.isAvailable()) {
            throw new IllegalStateException(
                    "gRPC client not initialised — cannot run the reasoner remotely");
        }
        IkeAdminGrpc.IkeAdminBlockingStub stub =
                IkeAdminGrpc.newBlockingStub(GrpcSearchClient.get().channel());

        LOG.info("Requesting remote reasoner run");
        Thread watcher = startCancelWatcher(tracker, stub);
        try {
            Iterator<RunReasonerEvent> events =
                    stub.runReasoner(RunReasonerRequest.getDefaultInstance());
            return toOutcome(readEvents(events, onPhase));
        } catch (StatusRuntimeException e) {
            // The stream can still end early if this thread is interrupted by Komet's own cancel.
            // By then the cancel has been sent, so report it as the cancel it is.
            if (e.getStatus().getCode() == Status.Code.CANCELLED && tracker != null && tracker.isCancelled()) {
                CancellationException cancelled = new CancellationException("Remote reasoner run was cancelled");
                cancelled.initCause(e);
                throw cancelled;
            }
            throw e;
        } finally {
            // Not once cancelled: the watcher may still be sending the cancel, and interrupting
            // it would drop the one call that stops the run. It exits on its own after sending.
            if (watcher != null && !tracker.isCancelled()) {
                watcher.interrupt();
            }
        }
    }

    private static RunReasonerResult readEvents(Iterator<RunReasonerEvent> events, PhaseListener onPhase) {
        RunReasonerResult result = null;
        while (events.hasNext()) {
            RunReasonerEvent event = events.next();
            if (event.hasRun()) {
                if (!event.getRun().getStarted()) {
                    LOG.info("Joined the remote reasoner run already in progress, started at {}",
                            Instant.ofEpochMilli(event.getRun().getStartedAt()));
                }
            } else if (event.hasPhase()) {
                var phase = event.getPhase();
                LOG.info("Reasoner step {} of {}: {}",
                        phase.getStep(), phase.getTotalSteps(), phase.getMessage());
                if (onPhase != null) {
                    onPhase.onPhase(phase.getStep(), phase.getTotalSteps(), phase.getMessage());
                }
            } else if (event.hasResult()) {
                result = event.getResult();
            }
        }
        return result;
    }

    /**
     * Sends {@code CancelReasoner} once {@code tracker} is cancelled.
     *
     * <p>A poller because the calling thread is blocked in the stream iterator for the length of
     * a phase — minutes on a large dataset — and cannot notice a cancel itself. Coarse on
     * purpose; a quarter-second is ample for stopping a run that long.
     */
    private static Thread startCancelWatcher(TrackingCallable<?> tracker, IkeAdminGrpc.IkeAdminBlockingStub stub) {
        if (tracker == null) {
            return null;
        }
        Thread watcher = new Thread(() -> {
            while (!tracker.isCancelled()) {
                try {
                    Thread.sleep(250L);
                } catch (InterruptedException e) {
                    return;
                }
            }
            LOG.info("Cancel requested — asking the server to stop the reasoner run");
            try {
                stub.cancelReasoner(CancelReasonerRequest.getDefaultInstance());
            } catch (StatusRuntimeException e) {
                // FAILED_PRECONDITION: the run ended before the cancel arrived. Its result is on
                // the way regardless, so there is nothing to do.
                LOG.info("Reasoner cancel not applied: {}", e.getStatus());
            }
        }, "grpc-reasoner-cancel-watcher");
        watcher.setDaemon(true);
        watcher.start();
        return watcher;
    }

    private static ReasonerOutcome toOutcome(RunReasonerResult result) {
        if (result == null) {
            throw new IllegalStateException(
                    "Reasoner stream ended without a result — the server closed it early");
        }
        if (result.getCancelled()) {
            LOG.info("Remote reasoner run cancelled after {}ms", result.getDurationMs());
            throw new CancellationException("Remote reasoner run was cancelled");
        }
        if (!result.getSuccess()) {
            throw new IllegalStateException("Remote reasoner failed: " + result.getErrorMessage());
        }

        LOG.info("Remote reasoner completed in {}ms: {} concepts classified",
                result.getDurationMs(), result.getCounts().getClassifiedConceptCount());

        return new ReasonerOutcome(
                result.getCounts().getClassifiedConceptCount(),
                toUuidLists(result.getConceptsWithInferredChangesList()),
                toUuidLists(result.getConceptsWithNavigationChangesList()),
                toUuidLists(result.getOrphansList()),
                result.getEquivalentSetsList().stream()
                        .map(set -> toUuidLists(set.getConceptList()))
                        .toList(),
                result.getCommitTime(),
                result.getStampCoordinateText(),
                result.getLogicCoordinateText(),
                result.getEditCoordinateText(),
                result.getDurationMs());
    }

    private static List<List<UUID>> toUuidLists(
            List<dev.ikm.tinkar.schema.PublicId> publicIds) {
        return publicIds.stream()
                .map(id -> id.getUuidsList().stream().map(UUID::fromString).toList())
                .toList();
    }
}
