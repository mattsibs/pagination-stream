package com.blog.stream.pagination;

import org.springframework.data.domain.Page;

public interface PageFetcher<T> {
    Page<T> fetch(final int offset, final int pageSize);
}
