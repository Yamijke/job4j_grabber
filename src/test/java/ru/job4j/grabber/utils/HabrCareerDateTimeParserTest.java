package ru.job4j.grabber.utils;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;

class HabrCareerDateTimeParserTest {

    @Test
    public void simpleTest() {
        HabrCareerDateTimeParser parser = new HabrCareerDateTimeParser();
        String input = "2023-09-25T23:58:15+03:00";
        LocalDateTime expected = LocalDateTime.of(2023, 9, 25, 23, 58, 15);
        LocalDateTime rsl = parser.parse(input);
        assertThat(expected).isEqualTo(rsl);
    }
}