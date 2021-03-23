package com.blog.stream.pagination;
import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * spliterator can iterate through page results for all pages sequentially,
 * or split on pages to allow fetching in parallel
 * @author ed.wenwtworth - inspired by/forked from https://github.com/mattsibs/pagination-stream
 * @param <P> page object returned
 * @param <T> type of object
 */
public class PageSpliterator<P, T> extends AbstractPageSpliterator<P, T> {

    private final int count;

    PageSpliterator(
            final int pageNumber,
            final int count,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemExtractor
    ) {
        super(pageNumber, pageSize, pageFetcher, itemExtractor);
        this.count = count;
    }

    /**
     * construct a spliterator given
     * @param count total pages expected
     * @param pageSize size of page
     * @param pageFetcher function given page number returns a page object of type P
     * @param itemExtractor function given a page object of type P return a list of type T
     * @param <P> page object type
     * @param <T> item type
     * @return created spliterator
     */
    public static <P, T> PageSpliterator<P, T> create(final int count, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<T>> itemExtractor) {
        return PageSpliterator.create(0, count, pageSize, pageFetcher, itemExtractor);
    }

    public static <P, T> PageSpliterator<P, T> create(final int pageNumber, final int count, final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher,Function<P, List<T>> itemExtractor) {
        return new PageSpliterator<>(pageNumber, count, pageSize, pageFetcher, itemExtractor);
    }

    /**
     * Attempt to split the spliterator. This is easy as by definition the result set is defined as paged buckets
     * <p>
     * If the stream is run in parallel the spliterator will be split
     * If run sequentially, try split will not be called and the tryAdvance handles paging
     *
     * @return Child spliterator whose role is to iterate over one set and die.
     */
    @Override
    public Spliterator<T> trySplit() {
        if (isLasPageOrGreater()) {
            return null;
        }

        ChildPageSpliterator<P, T> childSpliterator = new ChildPageSpliterator<>(getPageNumber(), getPageSize(), getPageFetcher(), getItemExtractor());
        incrementPageNumber();
        return childSpliterator;
    }

    private boolean isLasPageOrGreater() {
        return getPageSize() * (getPageNumber() + 1) >= getCount();
    }

    @Override
    public long estimateSize() {
        return getCount();
    }

    public int getCount() {
        return count;
    }
}

