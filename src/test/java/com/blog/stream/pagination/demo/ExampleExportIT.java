package com.blog.stream.pagination.demo;


import com.blog.stream.pagination.PaginationUtils;
import com.blog.stream.pagination.fixture.IntegrationTestApplication;
import com.blog.stream.pagination.fixture.User;
import com.blog.stream.pagination.fixture.UserRepository;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = IntegrationTestApplication.class)
public class ExampleExportIT {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Autowired
    private UserRepository userRepository;

    @Before
    public void setUp() {
        userRepository.deleteAll();
    }

    @Test
    public void example_parallel_ExportingWithLazyPaginatedStreams() throws Exception {
        createTestUsers(100);
        Stream<User> userStream = PaginationUtils.pagedStream(userRepository.pageFetcher(), userRepository.itemFetcher(), 7, 100);

        File file = temporaryFolder.newFile();
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            Exporter exporter = Exporter.create(outputStream);

            userStream
                    .parallel()
                    .map(UserExport::new)
                    .forEach(exporter::exportUser);
        }

        List<String> export = Files.lines(file.toPath())
                .collect(toList());

        assertThat(export)
                .hasSize(100)
                .containsOnlyOnce(
                        "AndroidInstance1,INDEX_1,2001-01-02",
                        "AndroidInstance87,INDEX_87,2001-03-29",
                        "AndroidInstance99,INDEX_99,2001-04-10"
                );

    }

    @Test
    public void example_sequential_ExportingWithLazyPaginatedStreams() throws Exception {
        createTestUsers(100);
        Stream<User> userStream = PaginationUtils.pagedStream(userRepository.pageFetcher(), userRepository.itemFetcher(), 7, 100);

        File file = temporaryFolder.newFile();
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            Exporter exporter = Exporter.create(outputStream);

            userStream
                    .sequential()
                    .map(UserExport::new)
                    .forEach(exporter::exportUser);
        }

        List<String> export = Files.lines(file.toPath())
                .collect(toList());

        assertThat(export)
                .hasSize(100)
                .startsWith(
                        "AndroidInstance0,INDEX_0,2001-01-01",
                        "AndroidInstance1,INDEX_1,2001-01-02",
                        "AndroidInstance2,INDEX_2,2001-01-03")
                .endsWith(
                        "AndroidInstance97,INDEX_97,2001-04-08",
                        "AndroidInstance98,INDEX_98,2001-04-09",
                        "AndroidInstance99,INDEX_99,2001-04-10");

    }

    private void createTestUsers(final int count) {
        List<User> users = IntStream.range(0, count)
                .boxed()
                .map(this::createRobotUser)
                .collect(toList());

        userRepository.saveAll(users);
    }

    private User createRobotUser(final int index) {
        User user = new User();
        user.setFirstName("AndroidInstance" + index);
        user.setLastName("INDEX_" + index);
        user.setDateOfBirth(LocalDate.of(2001, 1, 1).plusDays(index));
        return user;
    }
}
