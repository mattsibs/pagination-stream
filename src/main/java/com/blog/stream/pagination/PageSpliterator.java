package com.blog.stream.pagination;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.data.domain.Page;

import java.util.Spliterator;
import java.util.function.Consumer;

public class PageSpliterator<T> implements Spliterator<T> {

    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;

    private int pageNumber;
    private final int count;
    private final int pageSize;
    private final PageFetcher<T> pageFetcher;

    @VisibleForTesting
    PageSpliterator(
            final int pageNumber,
            final int count,
            final int pageSize,
            final PageFetcher<T> pageFetcher) {
        this.pageNumber = pageNumber;
        this.count = count;
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
    }

    static <R> PageSpliterator<R> create(final int count, final int pageSize, final PageFetcher<R> pageFetcher) {
        return new PageSpliterator<>(0, count, pageSize, pageFetcher);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        Page<T> page = pageFetcher.fetch(pageNumber, pageSize);
        page.forEach(action);

        //in parallel mode this section will always be run on the last page
        pageNumber++;
        return !page.isLast();
    }

    /**
     * Attempt to split the spliterator. Result set is explicitly defined as paged buckets
     *
     * If the stream is run in parallel the spliterator will be split as below
     * (4 queries for a given pageSize/count, top row is page number)
     * |   0    |   1    |   2    |   3    |
     * | Parent |        |        |        |  No Splits
     * | Child  | Parent |        |        |  First Split
     * | Child  | Child  | Parent |        |  Second Split
     * | Child  | Child  | Child  | Parent |  Third Split
     *                                        No more splits (return null)
     *
     *  If run sequentially, try split will not be called and the tryAdvance handles paging
     * @return Child spliterator whose role is to iterate over one set and die.
     */
    @Override
    public Spliterator<T> trySplit() {
        if (pageSize * (pageNumber + 1) >= count) {
            return null;
        }

        ChildPageSpliterator<T> childSpliterator = new ChildPageSpliterator<>(pageNumber, pageSize, pageFetcher);
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

    @VisibleForTesting
    int getPageNumber() {
        return pageNumber;
    }

    @VisibleForTesting
    static class ChildPageSpliterator<T> implements Spliterator<T> {

        private final int pageNumber;
        private final int pageSize;
        private final PageFetcher<T> pageFetcher;

        private ChildPageSpliterator(
                final int pageNumber,
                final int pageSize,
                final PageFetcher<T> pageFetcher) {
            this.pageNumber = pageNumber;
            this.pageSize = pageSize;
            this.pageFetcher = pageFetcher;
        }

        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
            Page<T> page = pageFetcher.fetch(pageNumber, pageSize);
            page.forEach(action);
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

        @VisibleForTesting
        int getPageNumber() {
            return pageNumber;
        }
    }
}
