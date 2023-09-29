package ru.job4j.grabber;

import ru.job4j.grabber.DateTimeParser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HabrCareerDateTimeParser implements DateTimeParser {

    @Override
    public LocalDateTime parse(String parse) {
        DateTimeFormatter dtf = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        return LocalDateTime.parse(parse, dtf);
    }
}
