package com.blog.stream.pagination.fixture;

import com.blog.stream.pagination.PageFetcher;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    default PageFetcher<User> pageFetcher() {
        return (offset, pageSize) -> {
            PageRequest pageable = PageRequest.of(offset, pageSize);
            return findAll(pageable);
        };
    }
}
