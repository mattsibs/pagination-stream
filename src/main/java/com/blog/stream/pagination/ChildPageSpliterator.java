package com.blog.stream.pagination;

import java.util.List;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Function;

class ChildPageSpliterator<P, T> extends AbstractPageSpliterator<P, T> implements Spliterator<T> {

    ChildPageSpliterator(
            final int pageNumber,
            final int pageSize,
            final BiFunction<Integer, Integer, P> pageFetcher,
            final Function<P, List<T>> itemExtractor
    ) {
        super(pageNumber, pageSize, pageFetcher, itemExtractor);
    }

    @Override
    protected boolean isLastPage(List<T> pageOfItems) {
        return true;
    }

    @Override
    public Spliterator<T> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return getPageSize();
    }

    @Override
    public int characteristics() {
        return ORDERED | IMMUTABLE | SIZED;
    }

}
