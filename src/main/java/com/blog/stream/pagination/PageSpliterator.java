package com.blog.stream.pagination;
import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * spliterator can iterate through page results for all pages sequentially,
 * or split on pages to allow fetching in parallel
 * @author ed.wenwtworth - inspired by/forked from https://github.com/mattsibs/pagination-stream
 * @param <P> page object returned
 * @param <T> type of object
 */
public class PageSpliterator<P, T> implements Spliterator<T> {

    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;

    private int pageNumber;
    private final int count;
    private final int pageSize;
    private final BiFunction<Integer, Integer, P> pageFetcher;
    private final Function<P, List<T>> itemFetcher;

    PageSpliterator(
            final int pageNumber,
            final int count,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemFetcher
    ) {
        this.pageNumber = pageNumber;
        this.count = count;
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
        this.itemFetcher = itemFetcher;
    }

    /**
     * construct a spliterator given
     * @param count total pages expected
     * @param pageSize size of page
     * @param pageFetcher function given page number returns a page object of type P
     * @param itemFetcher function given a page object of type P return a list of type T
     * @param <P>
     * @param <T>
     * @return created spliterator
     */
    static <P, T> PageSpliterator<P, T> create(final int count, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemFetcher) {
        return PageSpliterator.create(0, count, pageSize, pageFetcher, itemFetcher);
    }

    static <P, T> PageSpliterator<P, T> create(final int pageNumber, final int count, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher,Function<P, List<T>> itemFetcher) {
        return new PageSpliterator<>(pageNumber, count, pageSize, pageFetcher, itemFetcher);
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
     * @param action
     * @return
     */
    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        P page = pageFetcher.apply(pageNumber, pageSize);
        List<T> objects = itemFetcher.apply(page);
        objects.forEach(action);


        //in parallel mode this section will always be run on the last page
        pageNumber++;
        return !objects.isEmpty();


    }

    /**
     * Attempt to split the spliterator. This is easy as by definition the result set is defined as paged buckets
     * <p>
     * If the stream is run in parallel the spliterator will be split as below
     * (4 queries for a given pageSize/count, top row is page number)
     * |   0    |   1    |   2    |   3    |
     * | Parent |        |        |        |  No Splits
     * | Child  | Parent |        |        |  First Split
     * | Child  | Child  | Parent |        |  Second Split
     * | Child  | Child  | Child  | Parent |  Third Split
     * No more splits (return null)
     * <p>
     * If run sequentially, try split will not be called and the tryAdvance handles paging
     *
     * @return Child spliterator whose role is to iterate over one set and die.
     */
    @Override
    public Spliterator<T> trySplit() {
        if (pageSize * (pageNumber + 1) >= count) {
            return null;
        }

        ChildPageSpliterator<P, T> childSpliterator = new ChildPageSpliterator<>(pageNumber, pageSize, pageFetcher, itemFetcher);
        this.pageNumber++;
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        return count;
    }

    @Override
    public int characteristics() {
        return PAGED_SPLITERATOR_CHARACTERISTICS;
    }

    int getPageNumber() {
        return pageNumber;
    }

    static class ChildPageSpliterator<P, T> implements Spliterator<T> {

        private final int pageNumber;
        private final int pageSize;
        private final BiFunction<Integer, Integer, P> pageFetcher;
        private final Function<P, List<T>> itemFetcher;

        private ChildPageSpliterator(
                final int pageNumber,
                final int pageSize,
                final BiFunction<Integer, Integer, P> pageFetcher,
                final Function<P, List<T>> itemFetcher
        ) {
            this.pageNumber = pageNumber;
            this.pageSize = pageSize;
            this.pageFetcher = pageFetcher;
            this.itemFetcher = itemFetcher;
        }

        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
            P page = pageFetcher.apply(pageNumber, pageSize);
            List<T> objects = itemFetcher.apply(page);
            objects.forEach(action);
            return false;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return pageSize;
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE | SIZED;
        }

        int getPageNumber() {
            return pageNumber;
        }
    }
}

