package com.blog.stream.pagination;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Rule;
import org.junit.Test;

import java.util.Spliterator;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class PageSpliteratorTest {

    @Rule
    public JUnitSoftAssertions soft = new JUnitSoftAssertions();

    @Test
    public void trySplit_ReturnsChildWithCurrentPageAndMovesOnToNextPage() throws Exception {
        PageSpliterator<String> spliterator = new PageSpliterator<>(3, 100, 10, null);

        Spliterator<String> child = spliterator.trySplit();

        soft.assertThat(((PageSpliterator.ChildPageSpliterator) child).getPageNumber())
                .isEqualTo(3);

        soft.assertThat(spliterator.getPageNumber())
                .isEqualTo(4);


    }
}