package com.blog.stream.pagination;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.blog.stream.pagination.PageSpliterator.PAGED_SPLITERATOR_CHARACTERISTICS;

public final class PaginationUtils {
    private PaginationUtils() {

    }

    /**
     * Create lazily loaded stream for paginated queries. Stream type returned is sequential by default
     * performing pagedStream(...).parallel() will provided parallel stream.
     *
     * @param fetcher  Interface for retrieving pages
     * @param pageSize Size of pages to be queries
     * @param count    Overall size of result set, must be pre calculated
     * @param <T>      Generic type returned by page fetched
     * @return Stream of generic type T
     */
    public static <P, T> Stream<T> pagedStream(final BiFunction<Integer, Integer, P> fetcher, Function<P, List<T>> itemFetcher, final int pageSize, final int count) {
        PageSpliterator<P, T> spliterator = PageSpliterator.create(count, pageSize, fetcher, itemFetcher);
        return StreamSupport.stream(spliterator, false);
    }

    /**
     * To be used when calculation of count wants to be lazily evaluated.
     * <p>
     * Create lazily loaded stream for paginated queries. Stream type returned is sequential by default
     * performing pagedStream(...).parallel() will provided parallel stream.
     *
     * @param fetcher       Interface for retrieving pages
     * @param pageSize      Size of pages to be queries
     * @param countSupplier Method of obtaining count, will be lazily evaluated
     * @param <T>           Generic type returned by page fetched
     * @return Stream of generic type T
     */
    public static <P, T> Stream<T> pagedStream(
            final BiFunction<Integer, Integer, P> fetcher, Function<P, List<T>> itemFetcher, final int pageSize, final IntSupplier countSupplier) {

        Supplier<PageSpliterator<P, T>> spliterator = () -> PageSpliterator.create(countSupplier.getAsInt(), pageSize, fetcher, itemFetcher);

        return StreamSupport.stream(spliterator, PAGED_SPLITERATOR_CHARACTERISTICS, false);
    }


    /**
     * Stream over paginated result set without having to know the size of the result set beforehand.
     * First page is obtained when attempting to split.
     *
     * @param fetcher  Interface for retrieving pages
     * @param pageSize Size of pages to be queries
     * @param <T>      Generic type returned by page fetched
     * @return Stream of generic type T
     */
    public static <P, T> Stream<T> prefetchPageStream(final BiFunction<Integer, Integer, P> fetcher, Function<P, List<T>> itemFetcher, Function<P, Integer> pageCountExtractor, final int pageSize) {
        PreFetchPageSpliterator<P, T> spliterator = PreFetchPageSpliterator.create(pageSize, fetcher, itemFetcher, pageCountExtractor);
        return StreamSupport.stream(spliterator, false);
    }
}
