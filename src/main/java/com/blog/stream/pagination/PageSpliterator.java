package com.blog.stream.pagination;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * spliterator can iterate through page results for all pages sequentially,
 * or split on pages to allow fetching in parallel, but will get an accurate page count by prefetching
 *
 * @author ed.wenwtworth - inspired by/forked from https://github.com/mattsibs/pagination-stream
 * @param <P> page object returned
 * @param <T> type of object
 */
public class PageSpliterator<P, T> implements Spliterator<T> {
    private final AtomicInteger pageNumber;
    private final int limit;
    private final int pageSize;
    private final BiFunction<Integer, Integer, P> pageFetcher;
    private final Function<P, List<T>> itemExtractor;
    private final Function<P, Integer> totalPagesExtractor;

    private Integer totalNumberOfPages; //mutable (but only set once)
    private List<T> preFetchedPage;

    private PageSpliterator(
            final int pageNumber,
            final int limit,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemExtractor,
            final Function<P, Integer> totalPagesExtractor
            ) {
        this.pageNumber = new AtomicInteger(pageNumber);
        this.limit = limit;
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
        this.itemExtractor = itemExtractor;
        this.totalPagesExtractor = totalPagesExtractor;
    }

    /**
     * construct a spliterator for sequential, or parallel if a good estimate of the limit is known ahead of time
     * @param limit total pages expected
     * @param pageSize size of page
     * @param pageFetcher function given page number returns a page object of type P
     * @param itemExtractor function given a page object of type P return a list of type T
     * @param <P> page object type
     * @param <T> item type
     * @return created spliterator
     */
    public static <P, T> PageSpliterator<P, T> create(final int limit, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemExtractor) {
        return PageSpliterator.create(0, limit, pageSize, pageFetcher, itemExtractor);
    }

    /**
     * construct a spliterator for sequential, or parallel if a good estimate of the limit is known ahead of time
     * @param pageNumber what page to start on
     * @param limit total pages expected
     * @param pageSize size of page
     * @param pageFetcher function given page number returns a page object of type P
     * @param itemExtractor function given a page object of type P return a list of type T
     * @param <P> page object type
     * @param <T> item type
     * @return created spliterator
     */
    public static <P, T> PageSpliterator<P, T> create(final int pageNumber, final int limit, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemExtractor) {
        return new PageSpliterator<>(pageNumber, limit, pageSize, pageFetcher, itemExtractor, null);
    }

    /**
     * Construct a PageSpliterator for parallel with prefetch to get more accurate page limit (must have totalPagesExtractor)
     * @param pageSize size of each page
     * @param pageFetcher function to fetch a page of type P given both page and page size
     * @param itemExtractor function to extract list of item type T from page object of type P
     * @param totalPagesExtractor function to extract page limit from page of type P
     * @param <P> page object type
     * @param <T> item type
     * @return a PreFetchPageSpliterator configured for prefetch
     */
    public static <P, T> PageSpliterator<P, T> create(final int limit, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemExtractor, Function<P, Integer> totalPagesExtractor) {
        return new PageSpliterator<>(0, limit, pageSize, pageFetcher, itemExtractor, totalPagesExtractor);
    }

    @Override
    public Spliterator<T> trySplit() {

        if (isLastPageOrGreater()) {
            return null;
        }

        if (preFetchedPage != null && getPageNumber() == 0) {
            ChildPageSpliterator<List<T>, T> preFetchChildSpliterator = new ChildPageSpliterator<>(getPageNumber(), getPageSize(), (p, s) -> preFetchedPage, Function.identity());
            incrementPageNumber();
            return preFetchChildSpliterator;
        }
        ChildPageSpliterator<P, T> childSpliterator = new ChildPageSpliterator<>(getPageNumber(), getPageSize(), pageFetcher, itemExtractor);
        incrementPageNumber();
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        prefetchIfAllowed();

        if (totalNumberOfPages != null) {
            return Math.min(getLimit(), (long) getPageSize() * totalNumberOfPages);
        }
        return getLimit();
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        P page = pageFetcher.apply(getPageNumber(), getPageSize());
        List<T> pageOfItems = itemExtractor.apply(page);
        pageOfItems.forEach(action);
        incrementPageNumber();
        return !isLastPage(pageOfItems);
    }

    @Override
    public int characteristics() {
        return ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getPageNumber() {
        return pageNumber.get();
    }

    public Integer getLimit() {
        return this.limit;
    }

    /**
     * Create lazily loaded stream for paginated queries. Stream type returned is sequential by default
     * performing pagedStream(...).parallel() will provided parallel stream.
     *
     * @return Stream of generic type T
     */
    public Stream<T> stream() {
        return StreamSupport.stream(this, false).limit(getLimit());
    }

    protected boolean isLastPage(List<T> pageOfItems) {
        if (totalNumberOfPages != null && getPageNumber() == totalNumberOfPages) return true;
        return pageOfItems.size() < getPageSize()
                || (long) getPageNumber() * getPageSize() > estimateSize();
    }

    protected int incrementPageNumber() {
        return pageNumber.incrementAndGet();
    }

    private boolean isLastPageOrGreater() {
        if (totalNumberOfPages != null) {
            return getPageNumber() + 1 >= totalNumberOfPages;
        }
        return getPageSize() * (getPageNumber() + 1) >= getLimit();
    }

    private void prefetchIfAllowed() {
        if (totalPagesExtractor != null && totalNumberOfPages==null) {
            P page = pageFetcher.apply(getPageNumber(), getPageSize());
            preFetchedPage = itemExtractor.apply(page);
            totalNumberOfPages = totalPagesExtractor.apply(page);
        }
    }

    static class ChildPageSpliterator<P, T> implements Spliterator<T> {

        private final int pageNumber;
        private final int pageSize;
        private final BiFunction<Integer, Integer, P> pageFetcher;
        private final Function<P, List<T>> itemExtractor;

        ChildPageSpliterator(
                final int pageNumber,
                final int pageSize,
                final BiFunction<Integer, Integer, P> pageFetcher,
                final Function<P, List<T>> itemExtractor
        ) {
            this.pageNumber = pageNumber;
            this.pageSize = pageSize;
            this.pageFetcher = pageFetcher;
            this.itemExtractor = itemExtractor;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return getPageSize();
        }

        /**
         * continue advancing until no more pages
         *
         * @param action action to invoke on returned items
         * @return true if can advance
         */
        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
            P page = pageFetcher.apply(getPageNumber(), getPageSize());
            List<T> pageOfItems = itemExtractor.apply(page);
            pageOfItems.forEach(action);
            return false;
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE | SIZED;
        }

        public int getPageSize() {
            return pageSize;
        }

        public int getPageNumber() {
            return pageNumber;
        }

    }
}
