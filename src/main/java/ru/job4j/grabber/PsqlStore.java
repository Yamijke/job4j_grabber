package ru.job4j.grabber;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PsqlStore implements Store, AutoCloseable {

    private Connection cnn;

    public PsqlStore(Properties cfg) throws SQLException {
        try {
            Class.forName(cfg.getProperty("driver-class-name"));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        cnn = DriverManager.getConnection(
                cfg.getProperty("url"),
                cfg.getProperty("username"),
                cfg.getProperty("password")
        );
    }

    @Override
    public void save(Post post) {
        try (PreparedStatement ps = cnn.prepareStatement(
                "INSERT INTO post (name, text, link, created) VALUES (?, ?, ?, ?) ON CONFLICT (link) DO NOTHING;",
                Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, post.getTitle());
            ps.setString(2, post.getDescription());
            ps.setString(3, post.getLink());
            ps.setTimestamp(4, Timestamp.valueOf(post.getCreated()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Post> getAll() {
        List<Post> post = new ArrayList<>();
        try (PreparedStatement ps = cnn.prepareStatement("SELECT * FROM post")) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");
                String text = rs.getString("text");
                String link = rs.getString("link");
                Timestamp created = rs.getTimestamp("created");
                post.add(new Post(id, name, text, link, created.toLocalDateTime()));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return post;
    }

    @Override
    public Post findById(int id) {
        Post post = null;
        try (PreparedStatement ps = cnn.prepareStatement("SELECT * FROM post WHERE id = ?")) {
            ps.setInt(1, id);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int postId = rs.getInt("id");
                String name = rs.getString("name");
                String text = rs.getString("text");
                String link = rs.getString("link");
                Timestamp created = rs.getTimestamp("created");
                post = new Post(postId, name, text, link, created.toLocalDateTime());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return post;
    }

    @Override
    public void close() throws Exception {
        if (cnn != null) {
            cnn.close();
        }
    }

    public static void main(String[] args) throws SQLException {
        Properties prop = new Properties();
        try (InputStream in = PsqlStore.class.getClassLoader().getResourceAsStream("rabbit.properties")) {
            if (in == null) {
                System.out.println("Unable to find config file");
                return;
            }
            prop.load(in);
            PsqlStore psq = new PsqlStore(prop);
            Post post = new Post(1, "JavaMid", "link/111111111", "text about vacancy", LocalDateTime.now());
            Post post1 = new Post(2, "JavaJun", "link/22222222", "text about vacancy", LocalDateTime.now());
            Post post2 = new Post(3, "JavaSen", "link/33333333", "text about vacancy", LocalDateTime.now());
            Post post3 = new Post(4, "JavaMid", "link/111111111", "text about vacancy", LocalDateTime.now());
            psq.save(post);
            psq.save(post1);
            psq.save(post2);
            psq.save(post3);
            List<Post> posts = psq.getAll();
            for (Post pst : posts) {
                System.out.println(pst);
            }
            Post found = psq.findById(post2.getId());
            System.out.println(found);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
