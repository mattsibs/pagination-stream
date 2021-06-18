package com.blog.stream.pagination;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;

public class PageableSpliterator<T> implements Spliterator<T> {

    static final int PAGED_SPLITERATOR_CHARACTERISTICS = ORDERED | IMMUTABLE | SIZED | SUBSIZED | CONCURRENT;

    static <R> PageableSpliterator<R> create(final Pageable pageable, final Function<Pageable, Page<R>> pageFetcher) {
        return new PageableSpliterator<>(pageable, pageFetcher);
    }

    private final Function<Pageable, Page<T>> pageFetcher;
    private       Pageable                    pageable;
    private       Page<T>                     preFetchedPage;

    PageableSpliterator(
            final Pageable pageable,
            final Function<Pageable, Page<T>> pageFetcher) {
        this.pageable    = pageable;
        this.pageFetcher = pageFetcher;
    }

    @Override
    public boolean tryAdvance(final Consumer<? super T> action) {
        Page<T> page = pageFetcher.apply(pageable);
        page.forEach(action);

        //in parallel mode this section will always be run on the last page
        if (page.hasNext()) {
            pageable = pageable.next();
            return true;
        }
        return false;
    }

    @Override
    public Spliterator<T> trySplit() {
        if (preFetchedPage == null) {
            prefetchPage();
        }

        if (pageable.getPageNumber() + 1 >= preFetchedPage.getTotalPages()) {
            return null;
        }

        if (pageable.getPageNumber() == 0) {
            pageable = pageable.next();
            return new PreFetchedChildPageSpliterator<>(preFetchedPage);
        }

        ChildPageSpliterator<T> childSpliterator = new ChildPageSpliterator<>(pageable, pageFetcher);
        pageable = pageable.next();
        return childSpliterator;
    }

    @Override
    public long estimateSize() {
        if (preFetchedPage == null) {
            prefetchPage();
        }

        return preFetchedPage.getTotalElements();
    }

    @Override
    public int characteristics() {
        return PAGED_SPLITERATOR_CHARACTERISTICS;
    }

    private void prefetchPage() {
        preFetchedPage = pageFetcher.apply(pageable);
    }

    static class ChildPageSpliterator<T> implements Spliterator<T> {

        private final Pageable                    pageable;
        private final Function<Pageable, Page<T>> pageFetcher;

        private ChildPageSpliterator(
                final Pageable pageable,
                final Function<Pageable, Page<T>> pageFetcher) {
            this.pageable    = pageable;
            this.pageFetcher = pageFetcher;
        }

        @Override
        public boolean tryAdvance(final Consumer<? super T> action) {
            Page<T> page = pageFetcher.apply(pageable);
            page.forEach(action);
            return false;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return pageable.getPageSize();
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE | SIZED;
        }

    }

    static class PreFetchedChildPageSpliterator<T> implements Spliterator<T> {

        private final Page<T> page;

        PreFetchedChildPageSpliterator(final Page<T> page) {
            this.page = page;
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
            return page.getPageable().getPageSize();
        }

        @Override
        public int characteristics() {
            return ORDERED | IMMUTABLE | SIZED;
        }

    }
}
