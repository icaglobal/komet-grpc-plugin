package dev.ikm.tinkar.provider.grpc;

import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.entity.Entity;
import dev.ikm.tinkar.entity.EntityHandle;
import dev.ikm.tinkar.entity.PatternEntity;
import dev.ikm.tinkar.entity.SemanticEntity;

import java.util.Set;
import java.util.UUID;

/**
 * What Komet writes for itself into every session, and that is neither sent to the server nor
 * taken from it.
 *
 * <p>{@code PatternDefinitionSeeder} (komet/knowledge-layout) gives each session the
 * pattern-definition patterns and concepts its KL windows need, and projects every pattern the
 * store holds into semantics of those patterns. Against a local store that happens once. Over
 * gRPC the session's store starts empty, so the seeder writes it all every session. Its commit is
 * named, and {@link GrpcCommitForwarder} does not send it; but Komet can commit some of the same
 * versions again under an unnamed transaction (a tracked editable finds a difference that is not
 * one, such as "English Language" against "English language", and saves it after the commit).
 * Those reached the server, which then gave them back to every later session beside its own seed,
 * with versions whose stamps the session never loaded.
 *
 * <p>So the seeder's components are recognized by identity: the patterns and concepts of
 * {@code PatternDefinitionTerms} and {@code KlTerms}, and any semantic that refers to one of them,
 * or to such a semantic (a name of one of the concepts, a dialect of that name), or is a
 * semantic of one of the patterns.
 */
final class ClientLocalScaffolding {

    /** How far a chain of semantics referring to semantics is followed: dialect, name, concept. */
    private static final int MAX_DEPTH = 3;

    /** The seeder's patterns. */
    private static final Set<UUID> PATTERNS = Set.of(
            UUID.fromString("2f236377-2da7-49bf-8802-fd0fd2dfcdb5"),   // Meaning and Purpose Pattern
            UUID.fromString("8fafdacc-d1c0-4006-9965-53d3e05eaae7"));  // Fields Pattern

    /** The seeder's patterns and concepts. */
    private static final Set<UUID> IDENTITIES = Set.of(
            UUID.fromString("2f236377-2da7-49bf-8802-fd0fd2dfcdb5"),   // Meaning and Purpose Pattern
            UUID.fromString("8fafdacc-d1c0-4006-9965-53d3e05eaae7"),   // Fields Pattern
            UUID.fromString("03ac5b86-0965-4816-a5a2-e64aa8fe011e"),   // Pattern definition
            UUID.fromString("fdebcbf1-0b56-42da-8e6b-912d1e22f837"),   // Meaning and purpose
            UUID.fromString("8802ae3d-9af5-4d31-b69f-74766e70df16"),   // Field definition
            UUID.fromString("b39eb788-a8ba-4a82-8d29-884feab1f323"),   // Pattern meaning attribute
            UUID.fromString("7e718b8b-5017-40b3-8631-4b190fe139ac"),   // Pattern purpose attribute
            UUID.fromString("acc001dd-3430-42fb-ad6c-bf787a0211b9"),   // Field data type
            UUID.fromString("6da9d3ab-35dd-4046-a417-3473e8f7043b"),   // Field purpose
            UUID.fromString("728fa99a-4392-45b8-bfaf-084171cbed08"),   // Field meaning
            UUID.fromString("ac383ac9-6b2f-48f7-aa7d-5f7ce27b7ab7"));  // Field defaults module

    private ClientLocalScaffolding() {
    }

    /** Whether any of the UUIDs is one of the seeder's patterns or concepts. */
    static boolean isIdentity(UUID... uuids) {
        for (UUID uuid : uuids) {
            if (IDENTITIES.contains(uuid)) {
                return true;
            }
        }
        return false;
    }

    /** Whether any of the UUIDs is one of the seeder's patterns. */
    static boolean isPattern(UUID... uuids) {
        for (UUID uuid : uuids) {
            if (PATTERNS.contains(uuid)) {
                return true;
            }
        }
        return false;
    }

    /** Whether an entity in this session is the seeder's, or a semantic of or about the seeder's. */
    static boolean contains(Entity<?> entity) {
        return contains(entity, 0);
    }

    private static boolean contains(Entity<?> entity, int depth) {
        if (entity == null || depth > MAX_DEPTH) {
            return false;
        }
        if (isIdentity(entity.publicId().asUuidArray())) {
            return true;
        }
        if (entity instanceof SemanticEntity<?> semantic) {
            return isSeederNid(semantic.patternNid())
                    || isSeederNid(semantic.referencedComponentNid())
                    || contains(EntityHandle.get(semantic.referencedComponentNid()).orNull(), depth + 1);
        }
        return false;
    }

    /** Whether the component with this nid is one of the seeder's patterns or concepts, by its UUIDs. */
    static boolean isSeederNid(long nid) {
        try {
            return isIdentity(PrimitiveData.publicId(nid).asUuidArray());
        } catch (RuntimeException noPublicId) {
            return false;
        }
    }

    /** Whether a pattern in this session is one of the seeder's. */
    static boolean isSeederPattern(PatternEntity<?> pattern) {
        return isPattern(pattern.publicId().asUuidArray());
    }
}
