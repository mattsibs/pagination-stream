package com.blog.stream.pagination.fixture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    Logger LOG = LoggerFactory.getLogger(UserRepository.class);

    default BiFunction<Integer, Integer, PageRequest> pageFetcher() {
        return (pageNumber, pageSize) -> {
            LOG.info("Finding page for pageNumber {} and size {}", pageNumber, pageSize);
            return PageRequest.of(pageNumber, pageSize);
        };
    }

    default Function<PageRequest, List<User>> itemFetcher() {
        return (pageable) -> findAll(pageable).get().collect(Collectors.toList());
    }

    default Function<PageRequest, Integer> pageCountExtractor() {
        return (pageable) -> findAll(pageable).getTotalPages();
    }
}
