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

import dev.ikm.tinkar.common.service.LifecyclePhase;
import dev.ikm.tinkar.common.service.ServiceLifecycle;
import dev.ikm.tinkar.common.service.ServiceLifecyclePhase;
import dev.ikm.tinkar.common.util.broadcast.CommitBroadcaster;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityService;
import dev.ikm.tinkar.entity.StampEntity;
import dev.ikm.tinkar.entity.transform.EntityToTinkarSchemaTransformer;
import dev.ikm.tinkar.schema.TinkarMsg;
import dev.ikm.tinkar.service.proto.CommitEntitiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Sends locally-committed entities to the remote store.
 *
 * <p>In gRPC mode {@link GrpcPrimitiveDataService} is an in-memory store: a commit lands there
 * and nowhere else, so an edit is lost when the client exits and is invisible to every other
 * client and to search, which the server answers. This forwards each committed transaction on
 * to the server, which is the only durable store in this deployment.
 *
 * <p>Driven by {@link CommitBroadcaster} rather than by {@code merge()}, because a single
 * concept save produces several entities — the concept, its description semantic, its axiom
 * semantic, their stamps — as separate {@code merge()} calls. Forwarding per merge would let a
 * half-written concept reach the server; the broadcaster fires once per transaction, which is
 * the boundary that matters. Modelled on {@code CommitEventBridge}, which subscribes the same way.
 *
 * <p>Runs at {@link ServiceLifecyclePhase#CORE_SERVICES}, after {@code DATA_LOAD}, so the
 * initial dataset load is not re-sent to the server as a client edit.
 *
 * <p>Inert unless the gRPC client is initialised, so this is harmless when Komet is running
 * against a local datastore and the plugin merely happens to be on the path.
 */
@LifecyclePhase(value = ServiceLifecyclePhase.CORE_SERVICES, subPriority = 20)
public class GrpcCommitForwarder implements ServiceLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(GrpcCommitForwarder.class);

    private final Consumer<CommitBroadcaster.CommitNotification> listener = this::forward;

    @Override
    public void startup() {
        CommitBroadcaster.subscribe(listener);
        LOG.info("GrpcCommitForwarder installed: local commits will be sent to the remote store");
    }

    @Override
    public void shutdown() {
        CommitBroadcaster.unsubscribe(listener);
    }

    /**
     * Sends one committed transaction to the server.
     *
     * <p>Runs on the committing thread, which for a UI-driven save is the thread the user is
     * waiting on. That is deliberate: moving the call off-thread would let a failed remote
     * commit pass unnoticed while the local store still shows the edit as saved — the concept
     * looks written but exists only in this process. A visible pause is the better failure mode
     * until the editor can surface a proper progress and error state.
     */
    private void forward(CommitBroadcaster.CommitNotification notification) {
        if (!GrpcSearchClient.isAvailable()) {
            // Local datastore mode — the commit is already durable where it landed.
            return;
        }
        try {
            EntityToTinkarSchemaTransformer transformer = EntityToTinkarSchemaTransformer.getInstance();
            List<TinkarMsg> messages = new ArrayList<>();

            // Stamps first, matching the order the server stores them in: an entity version
            // cites its stamp, so the stamp has to be resolvable before that entity is read.
            for (int stampNid : notification.stampNids()) {
                StampEntity<?> stamp = EntityService.get().getStampFast(stampNid);
                if (stamp != null) {
                    messages.add(transformer.transform(stamp));
                }
            }
            for (int componentNid : notification.componentNids()) {
                Entity<?> entity = EntityService.get().getEntityFast(componentNid);
                if (entity != null) {
                    messages.add(transformer.transform(entity));
                }
            }

            if (messages.isEmpty()) {
                LOG.debug("Commit {} had no resolvable entities to send", notification.transactionUuid());
                return;
            }

            String name = notification.transactionName() == null || notification.transactionName().isBlank()
                    ? "Komet commit " + notification.transactionUuid()
                    : notification.transactionName();

            CommitEntitiesResponse response = GrpcSearchClient.get().commitEntities(messages, name);
            if (response.getSuccess()) {
                LOG.info("Committed {} entities to the remote store ({})",
                        response.getEntitiesCommitted(), name);
            } else {
                LOG.error("Remote store rejected commit '{}': {}", name, response.getErrorMessage());
            }
        } catch (Exception e) {
            // Never let a forwarding failure escape into the committing thread: the local
            // commit has already happened and cannot be undone from here.
            LOG.error("Failed to send commit {} to the remote store: {}",
                    notification.transactionUuid(), e.getMessage(), e);
        }
    }
}
