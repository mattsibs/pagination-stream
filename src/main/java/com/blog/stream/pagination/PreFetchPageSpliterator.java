package com.blog.stream.pagination;

import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class PreFetchPageSpliterator<P, T> implements Spliterator<T> {

    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;

    private int pageNumber;
    private int totalNumberOfPages;
    private boolean hasPrefetched;
    private final int pageSize;
    private final BiFunction<Integer, Integer, P> pageFetcher;
    private final Function<P, List<T>> itemExtractor;
    private final Function<P, Integer> totalPagesExtractor;

    private List<T> preFetchedPage;

    PreFetchPageSpliterator(
            final int pageNumber,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemExtractor,
            final Function<P, Integer> totalPagesExtractor
            ) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
        this.itemExtractor = itemExtractor;
        this.totalPagesExtractor = totalPagesExtractor;
    }

    static <P, R> PreFetchPageSpliterator<P, R> create(final int pageSize, final BiFunction<Integer, Integer, P> pageFetcher, Function<P, List<R>> itemFetcher, Function<P, Integer> totalPagesExtractor) {
        return new PreFetchPageSpliterator<>(0, pageSize, pageFetcher, itemFetcher, totalPagesExtractor);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        P page = pageFetcher.apply(pageNumber, pageSize);
        List<T> pageOfItems = itemExtractor.apply(page);
        pageOfItems.forEach(action);

        //in parallel mode this section will always be run on the last page
        pageNumber++;
        return !pageOfItems.isEmpty();
    }

    @Override
    public Spliterator<T> trySplit() {
        if (!hasPrefetched) {
            prefetchPage();
        }

        if (pageNumber == 0) {
            pageNumber++;
            return new PreFetchedChildPageSpliterator<>(preFetchedPage, pageSize);
        }

        if (pageNumber + 1 >= totalNumberOfPages) {
            return null;
        }

        ChildPageSpliterator<P, T> childSpliterator = new ChildPageSpliterator<>(pageNumber, pageSize, pageFetcher, itemExtractor);
        this.pageNumber++;
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        if (!hasPrefetched) {
            prefetchPage();
        }

        return (long)pageSize * totalNumberOfPages;
    }

    @Override
    public int characteristics() {
        return PAGED_SPLITERATOR_CHARACTERISTICS;
    }

    private void prefetchPage() {
        P page = pageFetcher.apply(pageNumber, pageSize);
        preFetchedPage = itemExtractor.apply(page);
        totalNumberOfPages = totalPagesExtractor.apply(page);
        hasPrefetched = true;
    }

    static class ChildPageSpliterator<P, T> implements Spliterator<T> {

        private final int pageNumber;
        private final int pageSize;
        private final BiFunction<Integer, Integer, P> pageFetcher;
        private final Function<P, List<T>> itemExtractor;

        private ChildPageSpliterator(
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
        public boolean tryAdvance(final Consumer<? super T> action) {
            P page = pageFetcher.apply(pageNumber, pageSize);
            List<T> pageOfItems = itemExtractor.apply(page);
            pageOfItems.forEach(action);
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

    }

    static class PreFetchedChildPageSpliterator<T> implements Spliterator<T> {

        private final List<T> page;
        private final int pageSize;

        PreFetchedChildPageSpliterator(final List<T> page, final int pageSize) {
            this.page = page;
            this.pageSize = pageSize;
        }


        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
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

    }
}
