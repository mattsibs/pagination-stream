package com.blog.stream.pagination;

import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * spliterator can iterate through page results for all pages sequentially,
 * or split on pages to allow fetching in parallel, but will get an accurate page count by prefetching
 *
 * @author ed.wenwtworth - inspired by/forked from https://github.com/mattsibs/pagination-stream
 * @param <P> page object returned
 * @param <T> type of object
 */
public class PreFetchPageSpliterator<P, T> extends AbstractPageSpliterator<P,T> implements Spliterator<T> {

    private int totalNumberOfPages;
    private boolean hasPrefetched;

    private final Function<P, Integer> totalPagesExtractor;

    private List<T> preFetchedPage;

    public PreFetchPageSpliterator(
            final int pageNumber,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemExtractor,
            final Function<P, Integer> totalPagesExtractor
            ) {
        super(pageNumber, pageSize, pageFetcher, itemExtractor);

        this.totalPagesExtractor = totalPagesExtractor;
    }

    /**
     * Construct a PreFetchPageSpliterator given
     * @param pageSize paging size
     * @param pageFetcher functional to fetch a page of type P given page and page size
     * @param itemExtractor extract list of item type T from page object of type P
     * @param totalPagesExtractor extract page count from page of type P
     * @param <P> page object type
     * @param <T> item type
     * @return a PreFetchPageSpliterator configured for prefetch
     */
    public static <P, T> PreFetchPageSpliterator<P, T> create(final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemExtractor, Function<P, Integer> totalPagesExtractor) {
        return new PreFetchPageSpliterator<>(0, pageSize, pageFetcher, itemExtractor, totalPagesExtractor);
    }

    @Override
    public Spliterator<T> trySplit() {
        if (!hasPrefetched) {
            prefetchPage();
        }

        if (getPageNumber() + 1 >= totalNumberOfPages) {
            return null;
        }

        ChildPageSpliterator<P, T> childSpliterator = new ChildPageSpliterator<>(getPageNumber(), getPageSize(), getPageFetcher(), getItemExtractor());
        incrementPageNumber();
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        if (!hasPrefetched) {
            prefetchPage();
        }

        return (long) getPageSize() * totalNumberOfPages;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        if (getPageNumber() == totalNumberOfPages) { // make sure the last iteration applies the prefetch page
            preFetchedPage.forEach(action);
            incrementPageNumber();
            return totalNumberOfPages != 1;
        } else {
            return super.tryAdvance(action);
        }
    }

    private void prefetchPage() {
        P page = getPageFetcher().apply(getPageNumber(), getPageSize());
        preFetchedPage = getItemExtractor().apply(page);
        totalNumberOfPages = totalPagesExtractor.apply(page);
        hasPrefetched = true;
    }
}
