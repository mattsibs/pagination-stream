# Pagination Stream
Java 8 Lazy Stream For Paginated Result Set.

Java Streams have a great api and provide the ability to lazily define loads an operations.
Creating your own lazy loaded stream is easy with the `Spliterator<T>` intergace.
This repo is an example of using the interface for loading paginated data.

## Usage
In order to use the utility, you need to define a `PageFetcher<T>` which defines how to load 
a paged result for a given offset and pagesize.

(Note the current interface for page uses the Spring Data object out of convenience)

### Example
```java
public class Main {
    
    
    public static void main(final String... args) {
        ...
        pagedStream(userRepository.pageFetcher(), 10, 100)
            .map(User::getName)
            .filter(name -> name.startsWith("A"))
            .count();
    }
}
```

Below is an example of a `PageFetcher` using spring data
```java
@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    default PageFetcher<User> pageFetcher() {
        return (offset, pageSize) -> {
            PageRequest pageable = PageRequest.of(offset, pageSize);
            return findAll(pageable);
        };
    }
}
```