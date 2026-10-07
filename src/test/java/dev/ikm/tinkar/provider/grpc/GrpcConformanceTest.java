package dev.ikm.tinkar.provider.grpc;

import dev.ikm.tinkar.fixtures.PrimitiveDataServiceConformance;
import dev.ikm.tinkar.fixtures.WithKeyValueProvider;

/**
 * The provider conformance suite (tinkar-core test-fixtures) against the gRPC provider. Every
 * store operation the suite exercises runs on the provider's local session store; the server is
 * reached only to fetch an entity missing from it, and for search. So no server is started: the
 * provider is pointed at a port nothing listens on, and a fetch for an entity it never stored
 * finds nothing, as it would on a server that does not have it.
 */
@WithKeyValueProvider(controllerClass = GrpcPrimitiveDataService.Controller.class)
class GrpcConformanceTest extends PrimitiveDataServiceConformance {

    static {
        // Port 1 refuses at once, so no test waits on a connection, and none reaches a real server.
        System.setProperty("komet.grpc.host", "localhost");
        System.setProperty("komet.grpc.port", "1");
    }
}
