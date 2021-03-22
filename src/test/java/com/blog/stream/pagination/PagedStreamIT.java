package com.blog.stream.pagination;

import com.blog.stream.pagination.fixture.IntegrationTestApplication;
import com.blog.stream.pagination.fixture.RandomUsers;
import com.blog.stream.pagination.fixture.User;
import com.blog.stream.pagination.fixture.UserRepository;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = IntegrationTestApplication.class)
public class PagedStreamIT {

    public static final int ITEM_COUNT = 100;
    public static final int PAGE_SIZE = 7;

    @Autowired
    private UserRepository userRepository;

    @Before
    public void setUp() {
        userRepository.deleteAll();
    }

    @Test
    public void pagedStream_parallel_IteratesOverWholeResultSet() {
        List<User> testUsers = createTestUsers(ITEM_COUNT);

        Set<Thread> threads = Sets.newHashSet();
        List<Long> streamedUserIds = PageSpliterator.create(ITEM_COUNT, PAGE_SIZE, userRepository.pageFetcher(), userRepository.itemFetcher()).stream()
                .parallel()
                .peek(user -> threads.add(Thread.currentThread()))
                .map(User::getId)
                .collect(toList());

        System.out.println("Concurrency: " + threads.size());

        assertThat(streamedUserIds)
                .containsExactlyInAnyOrderElementsOf(
                        testUsers.stream()
                                .map(User::getId)
                                .collect(toList()));

        assertThat(threads.size()).isGreaterThan(1);
    }

    @Test
    public void pagedStream_sequential_IteratesOverWholeResultSet() {
        List<User> testUsers = createTestUsers(ITEM_COUNT);

        Set<Thread> threads = Sets.newHashSet();

        List<Long> streamedUserIds = PageSpliterator.create(ITEM_COUNT, PAGE_SIZE, userRepository.pageFetcher(), userRepository.itemFetcher()).stream()
                .sequential()
                .peek(user -> threads.add(Thread.currentThread()))
                .map(User::getId)
                .collect(toList());

        System.out.println("Concurrency: " + threads.size());

        assertThat(streamedUserIds)
                .containsExactlyInAnyOrderElementsOf(
                        testUsers.stream()
                                .map(User::getId)
                                .collect(toList()));

        assertThat(threads.size()).isEqualTo(1);

    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void pagedStream_CountSupplier_NotCalledUntilTerminalMethod() {
        createTestUsers(10);

        AtomicInteger supplierCalledTimes = new AtomicInteger();
        Function<PageRequest, Integer> countSupplier = (p) -> {
            supplierCalledTimes.getAndIncrement();
            return 10;
        };

        assertThat(supplierCalledTimes.get())
                .isZero();

        PreFetchPageSpliterator.create(7, userRepository.pageFetcher(), userRepository.itemFetcher(), countSupplier).stream().count();

        assertThat(supplierCalledTimes.get())
                .isEqualTo(1);
    }


    @Test
    public void prefetchPageStream_parallel_IteratesOverWholeResultSet() {
        List<User> testUsers = createTestUsers(ITEM_COUNT);

        Set<Thread> threads = Sets.newHashSet();
        List<Long> streamedUserIds = PreFetchPageSpliterator.create(PAGE_SIZE, userRepository.pageFetcher(), userRepository.itemFetcher(), userRepository.pageCountExtractor()).stream()
                .parallel()
                .peek(user -> threads.add(Thread.currentThread()))
                .map(User::getId)
                .collect(toList());

        System.out.println("Concurrency: " + threads.size());

        assertThat(streamedUserIds)
                .containsExactlyInAnyOrderElementsOf(
                        testUsers.stream()
                                .map(User::getId)
                                .collect(toList()));

        assertThat(threads.size()).isGreaterThan(1);
    }

    @Test
    public void prefetchPageStream_sequential_IteratesOverWholeResultSet() {
        List<User> testUsers = createTestUsers(100);

        Set<Thread> threads = Sets.newHashSet();

        List<Long> streamedUserIds = PreFetchPageSpliterator.create(PAGE_SIZE, userRepository.pageFetcher(), userRepository.itemFetcher(), userRepository.pageCountExtractor()).stream().sequential()
                .peek(user -> threads.add(Thread.currentThread()))
                .map(User::getId)
                .collect(toList());

        System.out.println("Concurrency: " + threads.size());

        assertThat(streamedUserIds)
                .containsExactlyInAnyOrderElementsOf(
                        testUsers.stream()
                                .map(User::getId)
                                .collect(toList()));

        assertThat(threads.size()).isEqualTo(1);

    }

    private List<User> createTestUsers(final int count) {
        List<User> users = IntStream.range(0, count)
                .boxed()
                .map(x -> RandomUsers.createTestUser())
                .collect(toList());

        return userRepository.saveAll(users);
    }

}
