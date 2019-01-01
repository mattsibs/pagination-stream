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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = IntegrationTestApplication.class)
public class PagedStreamIT {

    @Autowired
    private UserRepository userRepository;

    @Before
    public void setUp() throws Exception {
        userRepository.deleteAll();
    }

    @Test
    public void pagedStream_parallel_IteratesOverWholeResultSet() throws Exception {
        List<User> testUsers = createTestUsers(100);

        Stream<User> userStream = PaginationUtils.pagedStream(userRepository.pageFetcher(), 7, 100);

        Set<Thread> threads = Sets.newHashSet();
        List<Long> streamedUserIds = userStream.parallel()
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
    public void pagedStream_sequential_IteratesOverWholeResultSet() throws Exception {
        List<User> testUsers = createTestUsers(100);

        Stream<User> userStream = PaginationUtils.pagedStream(userRepository.pageFetcher(), 7, 100);

        Set<Thread> threads = Sets.newHashSet();

        List<Long> streamedUserIds = userStream.sequential()
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
    public void pagedStream_CountSupplier_NotCalledUntilTerminalMethod() throws Exception {
        createTestUsers(10);

        AtomicInteger supplierCalledTimes = new AtomicInteger();
        Supplier<Integer> countSupplier = () -> {
            supplierCalledTimes.getAndIncrement();
            return 10;
        };

        Stream<User> userStream = PaginationUtils.pagedStream(userRepository.pageFetcher(), 7, countSupplier);

        assertThat(supplierCalledTimes.get())
                .isEqualTo(0);

        userStream.count();

        assertThat(supplierCalledTimes.get())
                .isEqualTo(1);
    }


    @Test
    public void prefetchPageStream_parallel_IteratesOverWholeResultSet() throws Exception {
        List<User> testUsers = createTestUsers(100);

        Stream<User> userStream = PaginationUtils.prefetchPageStream(userRepository.pageFetcher(), 7);

        Set<Thread> threads = Sets.newHashSet();
        List<Long> streamedUserIds = userStream.parallel()
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
    public void prefetchPageStream_sequential_IteratesOverWholeResultSet() throws Exception {
        List<User> testUsers = createTestUsers(100);

        Stream<User> userStream = PaginationUtils.prefetchPageStream(userRepository.pageFetcher(), 7);

        Set<Thread> threads = Sets.newHashSet();

        List<Long> streamedUserIds = userStream.sequential()
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
