package com.blog.stream.pagination;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.data.domain.Page;

import java.util.Spliterator;


public class PageSpliteratorTest {

    @Rule
    public final JUnitSoftAssertions soft = new JUnitSoftAssertions();

    @Test
    public void trySplit_ReturnsChildWithCurrentPageAndMovesOnToNextPage() {
        PageSpliterator<Page<String>, String> spliterator = PageSpliterator.create(3, 100, 10, null, null);

        Spliterator<String> child = spliterator.trySplit();

        soft.assertThat(((PageSpliterator.ChildPageSpliterator) child).getPageNumber())
                .isEqualTo(3);

        soft.assertThat(spliterator.getPageNumber())
                .isEqualTo(4);

    }

    @Test
    public void trySplit_ReturnsChildWithOnePageNoSplitAndNoMoveOnToNextPage() {
        PageSpliterator<Page<String>, String> spliterator = PageSpliterator.create(0, 10, 10, null, null);

        Spliterator<String> child = spliterator.trySplit();

        soft.assertThat(child).isNull();

        soft.assertThat(spliterator.getPageNumber())
                .isEqualTo(0);

    }
}