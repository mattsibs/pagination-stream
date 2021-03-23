package com.blog.stream.pagination;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractPageSpliterator<P, T> implements Spliterator<T> {
    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;
    private final int pageSize;
    private final BiFunction<Integer, Integer, P> pageFetcher;
    private final Function<P, List<T>> itemExtractor;
    private final AtomicInteger pageNumber;

    protected AbstractPageSpliterator(final int pageNumber, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, final Function<P, List<T>> itemExtractor) {
        this.pageNumber = new AtomicInteger(pageNumber);
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
        this.itemExtractor = itemExtractor;
    }

    /**
     * Create lazily loaded stream for paginated queries. Stream type returned is sequential by default
     * performing pagedStream(...).parallel() will provided parallel stream.
     *
     * @return Stream of generic type T
     */
    public Stream<T> stream() {
        return StreamSupport.stream(this, false);
    }

    /**
     * continue advancing until no more pages
     *
     * @param action action to invoke on returned items
     * @return true if can advance
     */
    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        P page = getPageFetcher().apply(getPageNumber(), getPageSize());
        List<T> pageOfItems = getItemExtractor().apply(page);
        pageOfItems.forEach(action);
        incrementPageNumber();
        return !isLastPage(pageOfItems);
    }

    @Override
    public int characteristics() {
        return AbstractPageSpliterator.PAGED_SPLITERATOR_CHARACTERISTICS;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getPageNumber() {
        return pageNumber.get();
    }

    protected BiFunction<Integer, Integer, P> getPageFetcher() {
        return pageFetcher;
    }

    protected Function<P, List<T>> getItemExtractor() {
        return itemExtractor;
    }

    protected boolean isLastPage(List<T> pageOfItems) {
        return pageOfItems.isEmpty()
                || pageOfItems.size() < getPageSize()
                || (long) getPageNumber() * getPageSize() > estimateSize();
    }

    protected int incrementPageNumber() {
        return pageNumber.incrementAndGet();
    }
}
