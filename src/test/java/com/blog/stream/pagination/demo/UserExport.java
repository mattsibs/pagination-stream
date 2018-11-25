package com.blog.stream.pagination.demo;

import com.blog.stream.pagination.fixture.User;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;

import java.time.LocalDate;

@JsonPropertyOrder({"firstName", "lastName", "dateOfBirth"})
public class UserExport {
    private String firstName;
    private String lastName;
    @JsonSerialize(using = LocalDateSerializer.class)
    private LocalDate dateOfBirth;

    public UserExport(final User user) {
        this.firstName = user.getFirstName();
        this.lastName = user.getLastName();
        this.dateOfBirth = user.getDateOfBirth();
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public LocalDate getDateOfBirth() {
        return dateOfBirth;
    }
}
