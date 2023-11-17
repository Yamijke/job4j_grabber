package ru.job4j.cache.menu;

import ru.job4j.cache.DirFileCache;

import java.io.File;
import java.util.Scanner;

public class Emulator {

    private static final String BASE_DIRECTORY = "C:\\Projects\\job4j_grabber\\src\\main\\java\\ru\\job4j\\cache\\txtFiles";

    public static final int CHOOSEANDLOADDIR = 1;
    public static final int READCONTENT = 2;
    public static final String SELECT = "Выберите меню";
    public static final String CHOOSE = "Укажите имя файла для загрузки в кэш";
    public static final String LOADED = "Данные файла занесены в кэш";
    public static final String READ = "Получите содержимое файла из кэша";

    public static final String MENU = """
                Введите 1 для указания пути к файлу для загрузки в кэш.
                Введите 2, чтобы получить содержимое файла из кэша.
                Введите любое другое число для выхода.
            """;

    public static void main(String[] args) {
        boolean run = true;
        DirFileCache dirFileCache = new DirFileCache(BASE_DIRECTORY);
        Scanner scanner = new Scanner(System.in);
        String fileName = null;
        while (run) {
            System.out.println(MENU);
            System.out.println(SELECT);
            try {
                int userChoice = Integer.parseInt(scanner.nextLine());
                System.out.println(userChoice);
                if (CHOOSEANDLOADDIR == userChoice) {
                    System.out.println(CHOOSE);
                    fileName = scanner.nextLine();
                    System.out.println(LOADED);
                } else if (READCONTENT == userChoice) {
                    System.out.println(READ);
                    if (fileName != null) {
                        String content = dirFileCache.get(fileName);
                        System.out.println(content);
                    } else {
                        System.out.println("Кэш не инициализирован или путь к файлу не указан.");
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println("Введите число");
            }
        }
        scanner.close();
    }
}
