package com.blog.stream.pagination;

import org.assertj.core.util.Sets;
import org.h2.mvstore.ConcurrentArrayList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

public class PageSpliteratorTest {

    static Logger LOG = LoggerFactory.getLogger(PageSpliteratorTest.class);

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

        Assert.assertEquals(3, ((PageSpliterator.ChildPageSpliterator<PagedResult<TestObject>, TestObject>) child).getPageNumber());
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
        List<TestObject> source = testObjectPagedRepository.source;

        assertAllIdsMatch(source, results);
    }

    @Test
    public void getPagedStreamShouldStreamThroughAllPages() {

        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(0, 1000, 100, testObjectPagedRepository::fetchPage, PagedResult::getItems);

        List<TestObject> allResults = spliterator.stream().collect(Collectors.toList());

        assertAllIdsMatch(source, allResults);

    }

    @Test
    public void getPagedStreamParallelShouldStreamThroughAllPagesUpToKnownThreadsInPool() throws Exception {

        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(1000, 10, testObjectPagedRepository::fetchPage, PagedResult::getItems);
        Set<Thread> threads = Sets.newHashSet();

        ForkJoinPool pool = new ForkJoinPool(100);

        ConcurrentArrayList<TestObject> allResultsc = new ConcurrentArrayList<>();

        Stream<TestObject> peekProcessor = spliterator.stream().parallel()
                .peek(user -> threads.add(Thread.currentThread()));

        pool.submit(() -> peekProcessor.forEach(allResultsc::add));
        pool.shutdown();
        Assert.assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        System.out.println("Concurrency: " + threads.size());
        System.out.println("avail processors: " + Runtime.getRuntime().availableProcessors());

        List<TestObject> allResults = new ArrayList<>();

        allResultsc.iterator().forEachRemaining(allResults::add);
        assertAllIdsMatch(allResults, testObjectPagedRepository.source);
        Assert.assertTrue(threads.size() > 1);

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
        System.out.println("avail processors: " + Runtime.getRuntime().availableProcessors());

        assertAllIdsMatch(source, allResults);

        Assert.assertTrue(threads.size() > 1);

    }

    @Test
    public void getPagedStreamParallelWithPrefetchShouldStreamThroughAllPagesUpToKnownThreads() {

        doTest("count > page size, parallel", 100, 1000, true);
        doTest("count = page size, parallel",100, 100, true);
        doTest("count on less than page size, parallel",100, 99, true);
        doTest("count < page size, parallel",100, 10, true);
        doTest("count = 0, parallel",100, 0, true);

        doTest("count > page size, sequential", 100, 1000, false);
        doTest("count = page size, sequential",100, 100,false);
        doTest("count on less than page size, sequential",100, 99,false);
        doTest("count < page size, sequential",100, 10, false);
        doTest("count = 0, sequential",100, 0, false);
    }

    private void doTest(String title, int pageSize, int totalResults, boolean isParallel) {
        source = IntStream.range(0, totalResults).mapToObj(this::createTestObject).collect(Collectors.toList());
        PagedRepository<TestObject> repo = new PagedRepository<>(source);

        PageSpliterator<PagedResult<TestObject>, TestObject> spliterator = PageSpliterator.create(totalResults, pageSize, repo::fetchPage, PagedResult::getItems, PagedResult::getPages);
        Set<Thread> threads = Sets.newHashSet();

        List<TestObject> allResults;
        if (isParallel) {
            allResults = spliterator.stream()
                    .parallel()
                    .peek(user -> threads.add(Thread.currentThread()))
                    .collect(Collectors.toList());
        } else {
            allResults = spliterator.stream()
                    .peek(user -> threads.add(Thread.currentThread()))
                    .collect(Collectors.toList());
        }

        System.out.println(title + " = Concurrency: " + threads.size());

        assertAllIdsMatch(source, allResults);

    }

    private static void assertAllIdsMatch(List<TestObject> expected, List<TestObject> actual) {
        List<Integer> actualIds = actual.stream()
                .map(TestObject::getId)
                .collect(toList());
        List<Integer> expectedIds = expected.stream()
                .map(TestObject::getId)
                .collect(toList());
        assertThat(actualIds)
                .containsExactlyInAnyOrderElementsOf(
                        expectedIds);
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
