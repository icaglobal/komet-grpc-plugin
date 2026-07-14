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

/**
 * gRPC-backed implementation of {@link dev.ikm.tinkar.provider.search.SearchService}.
 *
 * <p>Provides {@link dev.ikm.tinkar.provider.grpc.GrpcSearchService}, which delegates
 * concept search to a remote tinkar-core gRPC service via
 * {@link dev.ikm.tinkar.provider.grpc.GrpcSearchClient}.
 *
 * <p>The gRPC runtime libraries (grpc-api, grpc-stub, grpc-protobuf, guava) are shaded
 * into this jar so that jlink sees a single named module.
 * grpc-netty-shaded (the transport) remains on the classpath as a runtime-only
 * automatic module discovered via ServiceLoader.
 */
import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.ServiceLifecycle;
import dev.ikm.tinkar.provider.grpc.GrpcPrimitiveDataService;
import io.grpc.LoadBalancerProvider;
import io.grpc.ManagedChannelProvider;
import io.grpc.NameResolverProvider;
import io.grpc.ServerProvider;

module dev.ikm.tinkar.provider.grpc {

    exports dev.ikm.tinkar.provider.grpc;
    // Generated proto/gRPC stub classes
    exports dev.ikm.tinkar.service.proto;

    // NOTE: grpc-netty-shaded/grpc-core/grpc-util sit on the classpath (unnamed module) and
    // directly reference (extend, in NettyChannelProvider's case) classes shaded into this
    // module from grpc-api/guava (io.grpc, com.google.common.base/collect/io/math/
    // util.concurrent/hash/primitives — confirmed exhaustively via `jdeps -verbose:package`).
    // `exports pkg to ALL-UNNAMED;` is NOT valid module-info.java syntax — ALL-UNNAMED is only
    // a valid target for the --add-exports command-line/JVM flag. That flag must be supplied
    // by whatever launches this module (see application/pom.xml's jlink <options>, and the
    // equivalent VM option needed in IDE run configurations), not declared here.

    // Protobuf runtime — JPMS-wrapped
    requires dev.ikm.jpms.protobuf;
    // Tinkar schema message classes (from Tinkar.proto)
    requires dev.ikm.tinkar.schema;
    // JPMS-wrapped javax.annotation
    requires dev.ikm.jpms.javax.annotation;

    // SearchService contract and PrimitiveDataSearchResult
    requires dev.ikm.tinkar.provider.search;
    requires dev.ikm.tinkar.common;
    // Entity module: TinkarSchemaToEntityTransformer + EntityService for concept loading
    requires dev.ikm.tinkar.entity;

    // Eclipse Collections API: ImmutableList, IntProcedure, Lists.immutable
    requires org.eclipse.collections.api;

    requires org.slf4j;

    // Shaded-in grpc-api/grpc-stub/grpc-protobuf/guava classes (now part of this module) use
    // java.util.logging (e.g. io.grpc.ManagedChannelRegistry) and sun.misc.Unsafe (guava
    // internals) — confirmed via `jdeps -s` against the shaded jar's extracted classes.
    requires java.logging;
    requires jdk.unsupported;

    // Register GrpcPrimitiveDataService.Controller as a PrimitiveDataService provider
    provides DataServiceController with GrpcPrimitiveDataService.Controller;
    provides ServiceLifecycle with GrpcPrimitiveDataService.Controller;

    // The shaded-in io.grpc.*Registry classes (ManagedChannelRegistry, NameResolverRegistry,
    // LoadBalancerRegistry, ServerRegistry) call ServiceLoader.load(...) for these SPIs at
    // runtime — confirmed via javap against the shaded classes (ConfiguratorRegistry does NOT
    // use this pattern, so no `uses` needed for it). The module that CALLS ServiceLoader.load
    // must declare `uses`, even though the implementations are registered by grpc-netty-shaded
    // (an unnamed-module/classpath jar) via its own META-INF/services entries.
    uses ManagedChannelProvider;
    uses NameResolverProvider;
    uses LoadBalancerProvider;
    uses ServerProvider;
}
