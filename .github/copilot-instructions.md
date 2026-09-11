# Copilot Instructions for `library-events-consumer`

## Project Context
- Stack: Spring Boot 4.1.1, Java 21
- Messaging: Spring Kafka (`spring-boot-starter-kafka`)
- Persistence: Spring Data JPA + H2 (`spring-boot-starter-data-jpa`, `h2`)
- Web/API: Spring Web MVC (`spring-boot-starter-webmvc`)
- Validation: Jakarta Validation (`spring-boot-starter-validation`)
- Testing: dedicated Spring Boot test starters for JPA, Kafka, Validation, and Web MVC

## Coding Conventions
1. Prefer immutable domain transfer shapes and command/query models.
2. Use Java `record` for simple immutable DTO-style data carriers when behavior is minimal.
3. Use class-based DTOs only when required (framework constraints, custom constructors, derived logic).
4. Do not use Lombok.
5. Use constructor injection; do not use `@Autowired` in production code.
6. `@Autowired` is allowed in tests when it improves readability and setup clarity.

## Design Guidelines
1. Keep business logic in services; keep controllers and Kafka listeners thin.
2. Validate external input early using Bean Validation annotations and explicit checks.
3. Map between persistence entities and DTO/record models explicitly (no hidden magic).
4. Keep side effects explicit and isolated (database writes, Kafka publishing, external calls).
5. Prefer small, focused classes with clear responsibilities.

## Spring and Dependency Injection
- Prefer `final` fields for injected dependencies.
- Use a single explicit constructor per component/service where practical.
- Avoid field injection.
- Keep bean wiring straightforward and discoverable.

## JPA and Persistence
- Use entities for persistence concerns only; do not expose entities directly through API boundaries.
- Keep entity mutation controlled and intentional.
- Prefer immutable request/response models around mutable persistence entities.

## Testing Conventions
- Follow existing Maven test split:
  - Unit tests: `mvn test` (Surefire, excludes `*IT.java`)
  - Integration tests: `mvn failsafe:integration-test failsafe:verify` (Failsafe, includes `*IT.java`)
  - Full verification: `mvn verify`
- Prefer focused unit tests for business logic and targeted integration tests for Kafka/JPA/Web boundaries.

## Best Practices for Generated Code
1. Produce production-ready code with explicit error handling and clear validation paths.
2. Reuse existing project patterns and naming conventions.
3. Avoid broad catch blocks that hide failures.
4. Keep APIs and listener contracts stable unless change is explicitly requested.
5. Add or update tests when behavior changes.

