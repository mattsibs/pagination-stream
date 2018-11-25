package com.blog.stream.pagination.fixture;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.time.LocalDate;

public class RandomUsers {

    private RandomUsers() {

    }

    public static User createTestUser() {
        User user = new User();
        user.setFirstName(RandomStringUtils.random(10));
        user.setLastName(RandomStringUtils.random(10));
        user.setDateOfBirth(randomDate());

        return user;
    }

    public static LocalDate randomDate() {
        int year = RandomUtils.nextInt(0, 2018);
        int month = RandomUtils.nextInt(1, 13);
        int day = RandomUtils.nextInt(1, 29);
        return LocalDate.of(year, month, day);
    }
}
