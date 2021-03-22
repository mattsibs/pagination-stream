# Pagination Stream
Java 8 Lazy Stream For Paginated Result Set.


Forked from https://github.com/mattsibs/pagination-stream (master) @ 2021-03-13

## changes
 * no dependency on the page object type - use a page extractor function to fetch a page, and items extractor to 
   fetch a collection from the page

Java Streams have a great api and provide the ability to lazily define loads an operations.
Creating your own lazy loaded stream is easy with the `Spliterator<T>` interface.
This repo is an example of using the interface for loading paginated data.

## Usage
In order to use the utility, you need to define a `PageFetcher<T>` which defines how to load 
a paged result for a given offset and pagesize.

(Note the current interface for page uses the Spring Data object out of convenience, this will be
made generic in the future release so adapters can bridge the utility's requirements)

### Example
```java
public class Main {
    
    public static void main(final String... args) {
        //...
        pagedStream(userRepository.pageFetcher(), 10, 100)
            .parallel()
            .map(HeavyLifting::aDifficultCalculation)
            .forEach(notifier::notify);
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
