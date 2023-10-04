package ru.job4j.grabber;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.time.LocalDateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HabrCareerParse implements Parse {

    private static final String SOURCE_LINK = "https://career.habr.com";

    private static final String PAGE_LINK = String.format("%s/vacancies/java_developer", SOURCE_LINK);

    private final DateTimeParser dateTimeParser;

    private static final AtomicInteger POST_ID = new AtomicInteger(0);

    public HabrCareerParse(DateTimeParser dateTimeParser) {
        this.dateTimeParser = dateTimeParser;
    }

    private String retrieveDescription(String link) {
        Connection connection = Jsoup.connect(link);
        Document document;
        try {
            document = connection.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Elements description = document.select(".faded-content__container");
        return description.text();
    }

    public static void main(String[] args) throws IOException {
        DateTimeParser dateTimeParser = new HabrCareerDateTimeParser();
        HabrCareerParse habr = new HabrCareerParse(dateTimeParser);
        List<Post> posts = habr.list(PAGE_LINK);
        posts.forEach(System.out::println);
    }

    @Override
    public List<Post> list(String link) throws IOException {
        List<Post> result = new ArrayList<>();
        int currentPage = 1;
        while (currentPage <= 5) {
            String url = PAGE_LINK + "?page=" + currentPage;
            Connection connection = Jsoup.connect(url);
            Document document = connection.get();
            Elements rows = document.select(".vacancy-card__inner");
            rows.forEach(row -> {
                int id = POST_ID.incrementAndGet();
                Element titleElement = row.select(".vacancy-card__title").first();
                Element linkElement = titleElement.child(0);
                String vacancyName = titleElement.text();
                Element vacancyElement = row.select(".basic-date").first();
                String vacancyDate = vacancyElement.attr("datetime");
                LocalDateTime dateRes = dateTimeParser.parse(vacancyDate);
                String vacLink = String.format("%s%s", SOURCE_LINK, linkElement.attr("href"));
                String description = retrieveDescription(vacLink);
                result.add(new Post(id, vacancyName, vacLink, description, dateRes));
            });
            currentPage++;
        }
        return result;
    }
}
