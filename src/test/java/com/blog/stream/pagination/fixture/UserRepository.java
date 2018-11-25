package com.blog.stream.pagination.fixture;

import com.blog.stream.pagination.PageFetcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    Logger LOG = LoggerFactory.getLogger(UserRepository.class);

    default PageFetcher<User> pageFetcher() {
        return (pageNumber, pageSize) -> {
            LOG.info("Finding page for pageNumber {} and size {}", pageNumber, pageSize);
            PageRequest pageable = PageRequest.of(pageNumber, pageSize);
            return findAll(pageable);
        };
    }
}
