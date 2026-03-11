package com.shopifybot.db;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class Database {
    private final String jdbcUrl;

    public Database(String sqlitePath) {
        this.jdbcUrl = "jdbc:sqlite:" + sqlitePath;
    }

    public void init() {
        ensureParentDir();
        try (Connection conn = connect(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS posts (" +
                    "channel_id TEXT NOT NULL," +
                    "message_id INTEGER NOT NULL," +
                    "media_group_id TEXT," +
                    "text TEXT," +
                    "date INTEGER," +
                    "status TEXT NOT NULL," +
                    "product_id INTEGER," +
                    "last_update INTEGER," +
                    "PRIMARY KEY(channel_id, message_id)" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS media_items (" +
                    "media_group_id TEXT NOT NULL," +
                    "message_id INTEGER NOT NULL," +
                    "file_id TEXT NOT NULL," +
                    "file_unique_id TEXT," +
                    "file_size INTEGER," +
                    "width INTEGER," +
                    "height INTEGER," +
                    "PRIMARY KEY(media_group_id, message_id, file_id)" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS media_groups (" +
                    "media_group_id TEXT PRIMARY KEY," +
                    "channel_id TEXT NOT NULL," +
                    "last_update INTEGER NOT NULL," +
                    "processed INTEGER NOT NULL DEFAULT 0" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS collections (" +
                    "title TEXT PRIMARY KEY," +
                    "collection_id INTEGER NOT NULL," +
                    "handle TEXT" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS meta (" +
                    "key TEXT PRIMARY KEY," +
                    "value TEXT NOT NULL" +
                    ")");
            try {
                stmt.executeUpdate("ALTER TABLE collections ADD COLUMN handle TEXT");
            } catch (SQLException ignored) {
                // Column already exists.
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB init failed", e);
        }
    }

    private void ensureParentDir() {
        String path = jdbcUrl.replace("jdbc:sqlite:", "");
        java.io.File file = new java.io.File(path);
        java.io.File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
    }

    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    public void upsertPost(String channelId, long messageId, String mediaGroupId, String text, long date) {
        String sql = "INSERT INTO posts(channel_id, message_id, media_group_id, text, date, status, last_update) " +
                "VALUES(?,?,?,?,?,?,?) " +
                "ON CONFLICT(channel_id, message_id) DO UPDATE SET media_group_id=excluded.media_group_id, text=excluded.text, date=excluded.date, last_update=excluded.last_update";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, channelId);
            ps.setLong(2, messageId);
            ps.setString(3, mediaGroupId);
            ps.setString(4, text);
            ps.setLong(5, date);
            ps.setString(6, "NEW");
            ps.setLong(7, Instant.now().getEpochSecond());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB upsertPost failed", e);
        }
    }

    public void updatePostText(String channelId, long messageId, String text) {
        String sql = "UPDATE posts SET text=?, last_update=? WHERE channel_id=? AND message_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, text);
            ps.setLong(2, Instant.now().getEpochSecond());
            ps.setString(3, channelId);
            ps.setLong(4, messageId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB updatePostText failed", e);
        }
    }

    public void markPostStatus(String channelId, long messageId, String status) {
        String sql = "UPDATE posts SET status=?, last_update=? WHERE channel_id=? AND message_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, Instant.now().getEpochSecond());
            ps.setString(3, channelId);
            ps.setLong(4, messageId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB markPostStatus failed", e);
        }
    }

    public void markProductStatus(long productId, String status) {
        String sql = "UPDATE posts SET status=?, last_update=? WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, Instant.now().getEpochSecond());
            ps.setLong(3, productId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB markProductStatus failed", e);
        }
    }

    public void setProductIdForMediaGroup(String channelId, String mediaGroupId, long productId) {
        String sql = "UPDATE posts SET product_id=?, status=? WHERE channel_id=? AND media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productId);
            ps.setString(2, "PROCESSED");
            ps.setString(3, channelId);
            ps.setString(4, mediaGroupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB setProductIdForMediaGroup failed", e);
        }
    }

    public void setProductIdForMessage(String channelId, long messageId, long productId) {
        String sql = "UPDATE posts SET product_id=?, status=? WHERE channel_id=? AND message_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productId);
            ps.setString(2, "PROCESSED");
            ps.setString(3, channelId);
            ps.setLong(4, messageId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB setProductIdForMessage failed", e);
        }
    }

    public void clearProductForMessage(String channelId, long messageId) {
        String sql = "UPDATE posts SET product_id=NULL, status=? WHERE channel_id=? AND message_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "NEW");
            ps.setString(2, channelId);
            ps.setLong(3, messageId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB clearProductForMessage failed", e);
        }
    }

    public void clearProductForMediaGroup(String channelId, String mediaGroupId) {
        String sql = "UPDATE posts SET product_id=NULL, status=? WHERE channel_id=? AND media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "NEW");
            ps.setString(2, channelId);
            ps.setString(3, mediaGroupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB clearProductForMediaGroup failed", e);
        }
    }

    public Long findProductId(String channelId, long messageId) {
        String sql = "SELECT product_id FROM posts WHERE channel_id=? AND message_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, channelId);
            ps.setLong(2, messageId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long val = rs.getLong(1);
                    return rs.wasNull() ? null : val;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findProductId failed", e);
        }
        return null;
    }

    public Long findProductIdForMediaGroup(String mediaGroupId) {
        String sql = "SELECT product_id FROM posts WHERE media_group_id=? AND product_id IS NOT NULL LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long val = rs.getLong(1);
                    return rs.wasNull() ? null : val;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findProductIdForMediaGroup failed", e);
        }
        return null;
    }

    public List<PostRef> listProcessedPosts() {
        List<PostRef> items = new ArrayList<>();
        String sql = "SELECT channel_id, message_id, media_group_id, product_id FROM posts WHERE status='PROCESSED' AND product_id IS NOT NULL";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(new PostRef(
                            rs.getString(1),
                            rs.getLong(2),
                            rs.getString(3),
                            rs.getLong(4)
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB listProcessedPosts failed", e);
        }
        return items;
    }

    public void insertMediaItem(String mediaGroupId, long messageId, String fileId, String fileUniqueId, Integer fileSize, Integer width, Integer height) {
        String sql = "INSERT OR IGNORE INTO media_items(media_group_id, message_id, file_id, file_unique_id, file_size, width, height) VALUES(?,?,?,?,?,?,?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            ps.setLong(2, messageId);
            ps.setString(3, fileId);
            ps.setString(4, fileUniqueId);
            if (fileSize == null) ps.setNull(5, Types.INTEGER); else ps.setInt(5, fileSize);
            if (width == null) ps.setNull(6, Types.INTEGER); else ps.setInt(6, width);
            if (height == null) ps.setNull(7, Types.INTEGER); else ps.setInt(7, height);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB insertMediaItem failed", e);
        }
    }

    public void upsertMediaGroup(String mediaGroupId, String channelId) {
        String sql = "INSERT INTO media_groups(media_group_id, channel_id, last_update, processed) VALUES(?,?,?,0) " +
                "ON CONFLICT(media_group_id) DO UPDATE SET last_update=excluded.last_update";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            ps.setString(2, channelId);
            ps.setLong(3, Instant.now().getEpochSecond());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB upsertMediaGroup failed", e);
        }
    }

    public List<String> findReadyMediaGroups(long olderThanEpochSeconds) {
        List<String> ids = new ArrayList<>();
        String sql = "SELECT media_group_id FROM media_groups WHERE processed=0 AND last_update < ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, olderThanEpochSeconds);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findReadyMediaGroups failed", e);
        }
        return ids;
    }

    public void markMediaGroupProcessed(String mediaGroupId) {
        String sql = "UPDATE media_groups SET processed=1 WHERE media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB markMediaGroupProcessed failed", e);
        }
    }

    public boolean markMediaGroupProcessing(String mediaGroupId) {
        String sql = "UPDATE media_groups SET processed=2 WHERE media_group_id=? AND processed=0";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("DB markMediaGroupProcessing failed", e);
        }
    }

    public void markMediaGroupUnprocessed(String mediaGroupId) {
        String sql = "UPDATE media_groups SET processed=0 WHERE media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB markMediaGroupUnprocessed failed", e);
        }
    }

    public List<MediaItem> listMediaItems(String mediaGroupId) {
        List<MediaItem> items = new ArrayList<>();
        String sql = "SELECT message_id, file_id, file_unique_id, file_size, width, height " +
                "FROM media_items WHERE media_group_id=? ORDER BY message_id ASC";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    MediaItem item = new MediaItem(
                            rs.getLong(1),
                            rs.getString(2),
                            rs.getString(3),
                            (Integer) rs.getObject(4),
                            (Integer) rs.getObject(5),
                            (Integer) rs.getObject(6)
                    );
                    items.add(item);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB listMediaItems failed", e);
        }
        return items;
    }

    public List<Long> listMessageIdsForMediaGroup(String mediaGroupId) {
        List<Long> ids = new ArrayList<>();
        String sql = "SELECT DISTINCT message_id FROM media_items WHERE media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getLong(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB listMessageIdsForMediaGroup failed", e);
        }
        return ids;
    }

    public String findTextForMediaGroup(String mediaGroupId) {
        String sql = "SELECT text FROM posts WHERE media_group_id=? AND text IS NOT NULL AND text != '' LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findTextForMediaGroup failed", e);
        }
        return "";
    }

    public String findChannelIdForMediaGroup(String mediaGroupId) {
        String sql = "SELECT channel_id FROM media_groups WHERE media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findChannelIdForMediaGroup failed", e);
        }
        return null;
    }

    public void storeCollection(String title, long id, String handle) {
        String sql = "INSERT OR REPLACE INTO collections(title, collection_id, handle) VALUES(?,?,?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, title);
            ps.setLong(2, id);
            ps.setString(3, handle);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB storeCollection failed", e);
        }
    }

    public void clearCollections() {
        String sql = "DELETE FROM collections";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB clearCollections failed", e);
        }
    }

    public boolean hasMetaKey(String key) {
        String sql = "SELECT 1 FROM meta WHERE key=? LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB hasMetaKey failed", e);
        }
    }

    public void setMeta(String key, String value) {
        String sql = "INSERT OR REPLACE INTO meta(key, value) VALUES(?,?)";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB setMeta failed", e);
        }
    }

    public Long findCollectionId(String title) {
        String sql = "SELECT collection_id FROM collections WHERE title=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findCollectionId failed", e);
        }
        return null;
    }

    public String findCollectionHandle(String title) {
        String sql = "SELECT handle FROM collections WHERE title=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, title);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findCollectionHandle failed", e);
        }
        return null;
    }

    public static class MediaItem {
        public final long messageId;
        public final String fileId;
        public final String fileUniqueId;
        public final Integer fileSize;
        public final Integer width;
        public final Integer height;

        public MediaItem(long messageId, String fileId, String fileUniqueId, Integer fileSize, Integer width, Integer height) {
            this.messageId = messageId;
            this.fileId = fileId;
            this.fileUniqueId = fileUniqueId;
            this.fileSize = fileSize;
            this.width = width;
            this.height = height;
        }
    }

    public static class PostRef {
        public final String channelId;
        public final long messageId;
        public final String mediaGroupId;
        public final long productId;

        public PostRef(String channelId, long messageId, String mediaGroupId, long productId) {
            this.channelId = channelId;
            this.messageId = messageId;
            this.mediaGroupId = mediaGroupId;
            this.productId = productId;
        }
    }
}
