package dev.ikm.tinkar.provider.grpc;

import dev.ikm.tinkar.common.id.PublicId;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The store names any nid it minted, loaded or not: converting an entity to send to the server
 * asks for the public ids of everything it cites, a stamp this session never loaded among them.
 */
class PublicIdForNidTest {

    @Test
    void aMintedNidHasItsPublicIdWhetherOrNotItsEntityWasLoaded() {
        GrpcPrimitiveDataService store = GrpcPrimitiveDataService.provider();
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();

        long nid = store.nidForUuids(first, second);
        PublicId publicId = store.publicIdForNid(nid);

        assertEquals(Set.of(first, second), Set.copyOf(publicId.asUuidList().castToList()));
    }

    @Test
    void aNidNeverMintedHasNone() {
        GrpcPrimitiveDataService store = GrpcPrimitiveDataService.provider();
        long minted = store.nidForUuids(UUID.randomUUID());

        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> store.publicIdForNid(minted + 1_000_000));
        assertTrue(refused.getMessage().contains("No public id"), refused.getMessage());
    }
}
