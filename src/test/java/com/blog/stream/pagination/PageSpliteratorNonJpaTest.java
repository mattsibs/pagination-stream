package com.blog.stream.pagination;

import org.assertj.core.util.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class PageSpliteratorNonJpaTest {

    static Logger LOG = LoggerFactory.getLogger(PageSpliteratorNonJpaTest.class);

    PagedRepository<TestObject> testObjectPagedRepository;
    List<TestObject> source;

    TestObject createTestObject(int id) {
        return new TestObject(id, String.valueOf(id));
    }

    @Before
    public void setup() {
        source = IntStream.range(0, 1000).mapToObj(this::createTestObject).collect(Collectors.toList());
        testObjectPagedRepository = new PagedRepository<>(source);
    }

    @Test
    public void trySplitShouldReturnChildWithCurrentPageAndMoveOnToNextPage() {
        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(3, 100, 10, null, null);
        Spliterator<TestObject> child = spliterator.trySplit();

        Assert.assertEquals(3, ((ChildPageSpliterator<PagedResult<TestObject>, TestObject>) child).getPageNumber());
        Assert.assertEquals(4, spliterator.getPageNumber());

    }

    @Test
    public void trySplitAndForEachRemainingShouldIterateAndReturnAllObjects() {
        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(0, 1000, 100, testObjectPagedRepository::fetchPage, PagedResult::getItems);
        List<Spliterator<TestObject>> spliterators = new ArrayList<>();
        Spliterator<TestObject> child;
        do {
            child = spliterator.trySplit();
            if (child != null) spliterators.add(child);
        } while (child != null);

        spliterators.add(spliterator);
        Assert.assertEquals(10, spliterators.size());

        List<TestObject> results = new ArrayList<>();
        spliterators.forEach(s -> s.forEachRemaining(results::add));

        Assert.assertEquals(1000, results.size());

    }

    @Test
    public void getPagedStreamShouldStreamThroughAllPages() {

        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(0, 1000, 100, testObjectPagedRepository::fetchPage, PagedResult::getItems);

        List<TestObject> allResults = spliterator.stream().collect(Collectors.toList());

        Assert.assertEquals(source.size(), allResults.size());

    }

    @Test
    public void getPagedStreamParallelShouldStreamThroughAllPagesUpToKnownThreads() {

        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(1000, 100, testObjectPagedRepository::fetchPage, PagedResult::getItems);
        Set<Thread> threads = Sets.newHashSet();

        List<TestObject> allResults = spliterator.stream()
                .parallel()
                .peek(user -> threads.add(Thread.currentThread()))
       .collect(Collectors.toList());
        System.out.println("Concurrency: " + threads.size());

        Assert.assertEquals(source.size(), allResults.size());

        Assert.assertTrue(threads.size() > 1);

    }


    @Test
    public void getPagedStreamParallelWithPrefetchShouldStreamThroughAllPagesUpToKnownThreads() {
        doTest("count > page size", 100, 1000);
        doTest("count = page size",100, 100);
        doTest("count on less than page size",100, 99);
        doTest("count < page size",100, 10);
        doTest("count = 0",100, 0);
    }

    private void doTest(String title, int pageSize, int totalResults) {
        source = IntStream.range(0, totalResults).mapToObj(this::createTestObject).collect(Collectors.toList());
        PagedRepository<TestObject> repo = new PagedRepository<>(source);

        PreFetchPageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PreFetchPageSpliterator.create(pageSize, repo::fetchPage, PagedResult::getItems, PagedResult::getPages);
        Set<Thread> threads = Sets.newHashSet();

        List<TestObject> allResults = spliterator.stream()
                .parallel()
                .peek(user -> threads.add(Thread.currentThread()))
                .collect(Collectors.toList());
        System.out.println(title + " = Concurrency: " + threads.size());

        Assert.assertEquals(source.size(), allResults.size());

        assertThat(allResults).containsExactlyInAnyOrderElementsOf(source);

    }

    static class PagedResult<T> {
        private int pageNumber;
        private int total;
        private int pages;
        private int pageSize;
        private List<T> items;

        public PagedResult(List<T> items, int pageNumber, int pageSize, int total) {
            this.items = items;
            this.pages = total / pageSize + 1;
            this.pageSize = pageSize;
            this.pageNumber = pageNumber;
            this.total = total;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public void setPageNumber(int pageNumber) {
            this.pageNumber = pageNumber;
        }

        public int getTotal() {
            return total;
        }

        public void setTotal(int total) {
            this.total = total;
        }

        public int getPages() {
            return pages;
        }

        public void setPages(int pages) {
            this.pages = pages;
        }

        public List<T> getItems() {
            return items;
        }

        public void setItems(List<T> items) {
            this.items = items;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }
    }

    static class PagedRepository<T> {

        private final List<T> source;

        private Map<Integer, T> byIdIndex;

        PagedRepository(List<T> source) {
            this.source = source;
        }

        public PagedResult<T> fetchPage(int pageNumber, int pageSize) {
            LOG.info("Finding page for pageNumber {} and size {}", pageNumber, pageSize);

            int fromIndex = pageNumber * pageSize;
            int toIndex = Math.min(fromIndex + pageSize, source.size());

            List<T> items = (fromIndex <= toIndex) ? source.subList(fromIndex, toIndex) : Collections.emptyList();
            return new PagedResult<>(items, pageNumber, pageSize, source.size());
        }

    }

    public static class TestObject {
        private final Integer id;
        private final String val;

        public TestObject(int id, String val) {
            this.id = id;
            this.val = val;
        }

        public Integer getId() {
            return id;
        }

        public String getVal() {
            return val;
        }

        @Override
        public String toString() {
            return "TestObject{" +
                    "id=" + id +
                    ", val='" + val + '\'' +
                    '}';
        }
    }
}
