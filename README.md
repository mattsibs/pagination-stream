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
       PageSpliterator.create(ITEM_COUNT, PAGE_SIZE, UserRepository.pageFetcher(), UserRepository.itemFetcher()).stream()
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

    //fetch page from repository
   default BiFunction<Integer, Integer, PageRequest> pageFetcher() {
      return (pageNumber, pageSize) -> {
         LOG.info("Finding page for pageNumber {} and size {}", pageNumber, pageSize);
         return PageRequest.of(pageNumber, pageSize);
      };
   }

   //fetch items from page response
   default Function<PageRequest, List<User>> itemFetcher() {
      return (pageable) -> findAll(pageable).get().collect(Collectors.toList());
   }

   //fetch page count from page response -- for prefetch api
   default Function<PageRequest, Integer> pageCountExtractor() {
      return (pageable) -> findAll(pageable).getTotalPages();
   }
}
```
