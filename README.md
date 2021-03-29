# Pagination Stream
Java 8 Lazy Stream For Paginated Result Set.

Forked from https://github.com/mattsibs/pagination-stream (master) @ 2021-03-13

Java Streams have a great api and provide the ability to lazily define loads an operations.
Creating your own lazy loaded stream is easy with the `Spliterator<T>` interface.
This repo is an example of using the interface for loading paginated data.

## changes from fork
* no dependency on the page object type - use a page extractor function to fetch a page, and items extractor to 
  fetch a collection from the page
* combine to add prefetch capability into the pageSpliteratro

## how
the page spliterator is geared toward fetching pages of information from an API and returning as a stream of items. 
The undelying spliterator is subsised so that will create child splterators based on a constant page size for each 
page to be fetched up to the last page, which will iterate one item at a time. 

The choice can be made to make the stream parallel if it seems efficient to do so. The common pool can be used, 
but an custom pool can also be used. Since this opertion is likely I/O inensive rather than CPU, a custom pool can provide
more threads than available cores that limits the common pool, computing the thread size to be based on the blocking factor 
identified in typical cases. 

## benefits
Streaming results from api calls for further processing supports a more functional style of implementations. the algorythm
used to determine termination may take into account information from the first fetch such as number of pages, and/or also 
can be determined as pages are fetched. Further since the stream can terminate early if for instance findFirst or other 
terminating steps are added to minimize fetches to only those required. Finally Parallel implemnetation of the underlying 
spliterator for parallel streams may make fetching much faster depending on resource constraints.

## Usage
In order to use the utility, you need to define a few functionals:
* a page fetcher - function to fetch a paged result for a given page and pagesize
* a item extractor - function to get a list of items given a paged result object
* a page count extractor - function to get the number of pages that can be fetched (required for best parallel execution)

### Example
```java
public class Main {
    
    public static void main(final String... args) {
        //...
       PageSpliterator.create(LIMIT, PAGE_SIZE, UserRepository.pageFetcher(), UserRepository.itemExtractor(), UserRepository.pageCountExtractor).stream()
            .parallel()
            .map(HeavyLifting::aDifficultCalculation)
            .forEach(notifier::notify);
    }
}
```


Below is an example of functions required `pageFetcher`,`itemExtractor` and `pageCountExtractor`
using spring data

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
