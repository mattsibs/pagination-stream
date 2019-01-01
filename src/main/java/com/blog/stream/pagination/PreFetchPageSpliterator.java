package com.blog.stream.pagination;

import org.springframework.data.domain.Page;

import java.util.Spliterator;
import java.util.function.Consumer;

public class PreFetchPageSpliterator<T> implements Spliterator<T> {

    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;

    private int pageNumber;
    private int totalNumberOfPages;
    private boolean hasPrefetched;
    private final int pageSize;
    private final PageFetcher<T> pageFetcher;
    private Page<T> preFetchedPage;

    PreFetchPageSpliterator(
            final int pageNumber,
            final int pageSize,
            final PageFetcher<T> pageFetcher) {
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.pageFetcher = pageFetcher;
    }

    static <R> PreFetchPageSpliterator<R> create(final int pageSize, final PageFetcher<R> pageFetcher) {
        return new PreFetchPageSpliterator<>(0, pageSize, pageFetcher);
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        Page<T> page = pageFetcher.fetch(pageNumber, pageSize);
        page.forEach(action);

        //in parallel mode this section will always be run on the last page
        pageNumber++;
        return !page.isLast();
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

        ChildPageSpliterator<T> childSpliterator = new ChildPageSpliterator<>(pageNumber, pageSize, pageFetcher);
        this.pageNumber++;
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        if (!hasPrefetched) {
            prefetchPage();
        }

        return pageSize * totalNumberOfPages;
    }

    @Override
    public int characteristics() {
        return PAGED_SPLITERATOR_CHARACTERISTICS;
    }

    private void prefetchPage() {
        preFetchedPage = pageFetcher.fetch(pageNumber, pageSize);
        totalNumberOfPages = preFetchedPage.getTotalPages();
        hasPrefetched = true;
    }

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

    }

    static class PreFetchedChildPageSpliterator<T> implements Spliterator<T> {

        private final Page<T> page;
        private final int pageSize;

        PreFetchedChildPageSpliterator(final Page<T> page, final int pageSize) {
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
