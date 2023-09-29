package ru.job4j.grabber;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;

public class HabrCareerParse {

    private static final String SOURCE_LINK = "https://career.habr.com";

    private static final String PAGE_LINK = String.format("%s/vacancies/java_developer", SOURCE_LINK);

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
        HabrCareerParse habr = new HabrCareerParse();
        int currentPage = 1;
        while (currentPage <= 1) {
            String url = PAGE_LINK + "?page=" + currentPage;
            Connection connection = Jsoup.connect(url);
            Document document = connection.get();
            Elements rows = document.select(".vacancy-card__inner");
            rows.forEach(row -> {
                Element titleElement = row.select(".vacancy-card__title").first();
                Element linkElement = titleElement.child(0);
                String vacancyName = titleElement.text();
                Element vacancyElement = row.select(".basic-date").first();
                String vacancyDate = vacancyElement.attr("datetime");
                String link = String.format("%s%s", SOURCE_LINK, linkElement.attr("href"));
                String description = habr.retrieveDescription(link);
                System.out.printf("%s %s %s %s%n", vacancyName, link, vacancyDate, description);
            });
            currentPage++;
        }
    }
}
