# Pagination Stream
Java 8 Lazy Stream For Paginated Result Set.

Forked from https://github.com/mattsibs/pagination-stream (master) @ 2021-03-13

Java Streams have a great api and provide the ability to lazily define loads an operations.
Creating your own lazy loaded stream is easy with the `Spliterator<T>` interface.
This repo is an example of using the interface for loading paginated data.

## changes
* no dependency on the page object type - use a page extractor function to fetch a page, and items extractor to 
  fetch a collection from the page
* combine to add prefetch capability into the pageSpliteratro


## Usage
In order to use the utility, you need to define a few functionals:
* a page extractor - how to load a paged result for a given offset and pagesize.
* a itm extractor - how to get a list of items from a returned page
* a page count extractor - how to compute the number of pages that can be fetched (required for best parallel execution)

Two classes can be used in sequence or parallel
1. `PageSpliterator` - used if a good estimate of the size of the result is known
2. `PreFetchPagePliterator` - used if the first page fetched can get the estimate off the number of pages that can be returned

## To Do
- [ ] Can we use adapters to ccreae a PagedResult object that can be adapted without addingg item extractor and page count extrator?

### Example
```java
public class Main {
    
    public static void main(final String... args) {
        //...
       PageSpliterator.create(PAGE_SIZE, UserRepository.pageFetcher(), UserRepository.itemExtractor(), UserRepository.pageCountExtractor).stream()
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
   default Function<PageRequest, List<User>> itemExtractor() {
      return (pageable) -> findAll(pageable).get().collect(Collectors.toList());
   }

   //fetch page count from page response -- for prefetch api
   default Function<PageRequest, Integer> pageCountExtractor() {
      return (pageable) -> findAll(pageable).getTotalPages();
   }
}
```
