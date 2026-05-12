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
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS admin_users (" +
                    "user_id INTEGER PRIMARY KEY," +
                    "username TEXT," +
                    "added_at INTEGER NOT NULL," +
                    "added_by INTEGER" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS bot_users (" +
                    "user_id INTEGER PRIMARY KEY," +
                    "username TEXT," +
                    "first_name TEXT," +
                    "last_name TEXT," +
                    "is_admin INTEGER NOT NULL DEFAULT 0," +
                    "last_seen INTEGER NOT NULL" +
                    ")");
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS product_cards (" +
                    "product_id INTEGER PRIMARY KEY," +
                    "channel_id TEXT NOT NULL," +
                    "message_id INTEGER NOT NULL," +
                    "media_group_id TEXT," +
                    "title TEXT NOT NULL," +
                    "size TEXT," +
                    "description TEXT NOT NULL," +
                    "article TEXT NOT NULL UNIQUE," +
                    "base_price_rsd REAL NOT NULL," +
                    "current_price_rsd REAL NOT NULL," +
                    "discount_percent INTEGER NOT NULL DEFAULT 0," +
                    "fixed_price_rsd REAL," +
                    "status TEXT NOT NULL DEFAULT 'ACTIVE'," +
                    "created_at INTEGER NOT NULL," +
                    "updated_at INTEGER NOT NULL" +
                    ")");
            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_product_cards_status ON product_cards(status)");
            stmt.executeUpdate("CREATE INDEX IF NOT EXISTS idx_product_cards_updated_at ON product_cards(updated_at)");
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
        String sql = "SELECT DISTINCT message_id FROM media_items WHERE media_group_id=? " +
                "UNION SELECT DISTINCT message_id FROM posts WHERE media_group_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, mediaGroupId);
            ps.setString(2, mediaGroupId);
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

    public String getMeta(String key) {
        String sql = "SELECT value FROM meta WHERE key=? LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB getMeta failed", e);
        }
        return null;
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

    public void upsertUser(long userId, String username, String firstName, String lastName, boolean isAdmin) {
        String sql = "INSERT INTO bot_users(user_id, username, first_name, last_name, is_admin, last_seen) VALUES(?,?,?,?,?,?) " +
                "ON CONFLICT(user_id) DO UPDATE SET " +
                "username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name, " +
                "is_admin=excluded.is_admin, last_seen=excluded.last_seen";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setString(2, username);
            ps.setString(3, firstName);
            ps.setString(4, lastName);
            ps.setInt(5, isAdmin ? 1 : 0);
            ps.setLong(6, Instant.now().getEpochSecond());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB upsertUser failed", e);
        }
    }

    public boolean isAdmin(long userId) {
        String sql = "SELECT 1 FROM admin_users WHERE user_id=? LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB isAdmin failed", e);
        }
    }

    public void addAdmin(long userId, String username, Long addedBy) {
        long now = Instant.now().getEpochSecond();
        String adminSql = "INSERT INTO admin_users(user_id, username, added_at, added_by) VALUES(?,?,?,?) " +
                "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(adminSql)) {
            ps.setLong(1, userId);
            ps.setString(2, username);
            ps.setLong(3, now);
            if (addedBy == null) {
                ps.setNull(4, Types.INTEGER);
            } else {
                ps.setLong(4, addedBy);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB addAdmin failed", e);
        }

        String userSql = "INSERT INTO bot_users(user_id, username, first_name, last_name, is_admin, last_seen) VALUES(?,?,?,?,1,?) " +
                "ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, is_admin=1, last_seen=excluded.last_seen";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(userSql)) {
            ps.setLong(1, userId);
            ps.setString(2, username);
            ps.setString(3, "");
            ps.setString(4, "");
            ps.setLong(5, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB addAdmin(user upsert) failed", e);
        }
    }

    public int countUsers() {
        String sql = "SELECT COUNT(*) FROM bot_users";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB countUsers failed", e);
        }
    }

    public List<UserRecord> listUsers(int limit, int offset) {
        List<UserRecord> items = new ArrayList<>();
        String sql = "SELECT user_id, username, first_name, last_name, is_admin, last_seen " +
                "FROM bot_users ORDER BY last_seen DESC LIMIT ? OFFSET ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(new UserRecord(
                            rs.getLong(1),
                            rs.getString(2),
                            rs.getString(3),
                            rs.getString(4),
                            rs.getInt(5) == 1,
                            rs.getLong(6)
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB listUsers failed", e);
        }
        return items;
    }

    public int countVisibleProducts() {
        String sql = "SELECT COUNT(*) FROM product_cards WHERE status IN ('ACTIVE','RESERVED')";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB countVisibleProducts failed", e);
        }
    }

    public int countReservedProducts() {
        String sql = "SELECT COUNT(*) FROM product_cards WHERE status='RESERVED'";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB countReservedProducts failed", e);
        }
    }

    public List<ProductCard> listVisibleProducts(int limit, int offset) {
        return listProductsByStatuses(limit, offset, "status IN ('ACTIVE','RESERVED')");
    }

    public List<ProductCard> listReservedProducts(int limit, int offset) {
        return listProductsByStatuses(limit, offset, "status='RESERVED'");
    }

    public List<ProductCard> listProductsForDiscount() {
        return listProductsByStatuses(5000, 0, "status='ACTIVE'");
    }

    private List<ProductCard> listProductsByStatuses(int limit, int offset, String whereClause) {
        List<ProductCard> items = new ArrayList<>();
        String sql = "SELECT product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at " +
                "FROM product_cards WHERE " + whereClause + " ORDER BY created_at ASC LIMIT ? OFFSET ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, limit);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    items.add(mapProductCard(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB listProductsByStatuses failed", e);
        }
        return items;
    }

    public ProductCard findVisibleProductByOrdinal(int ordinal) {
        if (ordinal <= 0) return null;
        List<ProductCard> items = listVisibleProducts(1, ordinal - 1);
        return items.isEmpty() ? null : items.get(0);
    }

    public ProductCard findReservedProductByOrdinal(int ordinal) {
        if (ordinal <= 0) return null;
        List<ProductCard> items = listReservedProducts(1, ordinal - 1);
        return items.isEmpty() ? null : items.get(0);
    }

    public ProductCard findVisibleProductByArticle(String article) {
        String sql = "SELECT product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at " +
                "FROM product_cards WHERE article=? AND status IN ('ACTIVE','RESERVED') LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, article);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapProductCard(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findVisibleProductByArticle failed", e);
        }
        return null;
    }

    public ProductCard findReservedProductByArticle(String article) {
        String sql = "SELECT product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at " +
                "FROM product_cards WHERE article=? AND status='RESERVED' LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, article);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapProductCard(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findReservedProductByArticle failed", e);
        }
        return null;
    }

    public Integer findVisibleOrdinalByProductId(long productId) {
        ProductCard card = findProductCardById(productId);
        if (card == null) return null;
        String sql = "SELECT COUNT(*) FROM product_cards WHERE status IN ('ACTIVE','RESERVED') " +
                "AND (created_at < ? OR (created_at = ? AND product_id <= ?))";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, card.createdAt);
            ps.setLong(2, card.createdAt);
            ps.setLong(3, card.productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findVisibleOrdinalByProductId failed", e);
        }
        return null;
    }

    public Integer findReservedOrdinalByProductId(long productId) {
        ProductCard card = findProductCardById(productId);
        if (card == null) return null;
        String sql = "SELECT COUNT(*) FROM product_cards WHERE status='RESERVED' " +
                "AND (created_at < ? OR (created_at = ? AND product_id <= ?))";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, card.createdAt);
            ps.setLong(2, card.createdAt);
            ps.setLong(3, card.productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findReservedOrdinalByProductId failed", e);
        }
        return null;
    }

    public ProductCard findProductCardById(long productId) {
        String sql = "SELECT product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at " +
                "FROM product_cards WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapProductCard(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findProductCardById failed", e);
        }
        return null;
    }

    public ProductCard findProductCardByArticle(String article) {
        String sql = "SELECT product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at " +
                "FROM product_cards WHERE article=? LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, article);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return mapProductCard(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB findProductCardByArticle failed", e);
        }
        return null;
    }

    public boolean existsActiveArticle(String article) {
        String sql = "SELECT 1 FROM product_cards WHERE article=? AND status IN ('ACTIVE','RESERVED') LIMIT 1";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, article);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new RuntimeException("DB existsActiveArticle failed", e);
        }
    }

    public void upsertProductCard(ProductCard card) {
        long now = Instant.now().getEpochSecond();
        String sql = "INSERT INTO product_cards(product_id, channel_id, message_id, media_group_id, title, size, description, article, " +
                "base_price_rsd, current_price_rsd, discount_percent, fixed_price_rsd, status, created_at, updated_at) " +
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) " +
                "ON CONFLICT(product_id) DO UPDATE SET " +
                "channel_id=excluded.channel_id, message_id=excluded.message_id, media_group_id=excluded.media_group_id, " +
                "title=excluded.title, size=excluded.size, description=excluded.description, article=excluded.article, " +
                "base_price_rsd=excluded.base_price_rsd, current_price_rsd=excluded.current_price_rsd, " +
                "discount_percent=excluded.discount_percent, fixed_price_rsd=excluded.fixed_price_rsd, " +
                "status=excluded.status, updated_at=excluded.updated_at";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, card.productId);
            ps.setString(2, card.channelId);
            ps.setLong(3, card.messageId);
            ps.setString(4, card.mediaGroupId);
            ps.setString(5, card.title);
            ps.setString(6, card.size);
            ps.setString(7, card.description);
            ps.setString(8, card.article);
            ps.setDouble(9, card.basePriceRsd);
            ps.setDouble(10, card.currentPriceRsd);
            ps.setInt(11, card.discountPercent);
            if (card.fixedPriceRsd == null) {
                ps.setNull(12, Types.REAL);
            } else {
                ps.setDouble(12, card.fixedPriceRsd);
            }
            ps.setString(13, card.status);
            ps.setLong(14, card.createdAt <= 0 ? now : card.createdAt);
            ps.setLong(15, now);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB upsertProductCard failed", e);
        }
    }

    public void updateProductCardStatus(long productId, String status) {
        String sql = "UPDATE product_cards SET status=?, updated_at=? WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setLong(2, Instant.now().getEpochSecond());
            ps.setLong(3, productId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB updateProductCardStatus failed", e);
        }
    }

    public void updateProductCardReservation(long productId, boolean reserved) {
        updateProductCardStatus(productId, reserved ? "RESERVED" : "ACTIVE");
    }

    public void deleteProductCard(long productId) {
        String sql = "DELETE FROM product_cards WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, productId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB deleteProductCard failed", e);
        }
    }

    public void updateProductCardPricing(long productId, double currentPriceRsd, int discountPercent, Double fixedPriceRsd) {
        String sql = "UPDATE product_cards SET current_price_rsd=?, discount_percent=?, fixed_price_rsd=?, updated_at=? WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setDouble(1, currentPriceRsd);
            ps.setInt(2, discountPercent);
            if (fixedPriceRsd == null) {
                ps.setNull(3, Types.REAL);
            } else {
                ps.setDouble(3, fixedPriceRsd);
            }
            ps.setLong(4, Instant.now().getEpochSecond());
            ps.setLong(5, productId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB updateProductCardPricing failed", e);
        }
    }

    public void touchProductCardMessage(long productId, String channelId, long messageId, String mediaGroupId) {
        String sql = "UPDATE product_cards SET channel_id=?, message_id=?, media_group_id=?, updated_at=? WHERE product_id=?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, channelId);
            ps.setLong(2, messageId);
            ps.setString(3, mediaGroupId);
            ps.setLong(4, Instant.now().getEpochSecond());
            ps.setLong(5, productId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB touchProductCardMessage failed", e);
        }
    }

    private ProductCard mapProductCard(ResultSet rs) throws SQLException {
        Number fixedRaw = (Number) rs.getObject(12);
        Double fixed = fixedRaw == null ? null : fixedRaw.doubleValue();
        return new ProductCard(
                rs.getLong(1),
                rs.getString(2),
                rs.getLong(3),
                rs.getString(4),
                rs.getString(5),
                rs.getString(6),
                rs.getString(7),
                rs.getString(8),
                rs.getDouble(9),
                rs.getDouble(10),
                rs.getInt(11),
                fixed,
                rs.getString(13),
                rs.getLong(14),
                rs.getLong(15)
        );
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

    public static class UserRecord {
        public final long userId;
        public final String username;
        public final String firstName;
        public final String lastName;
        public final boolean admin;
        public final long lastSeen;

        public UserRecord(long userId, String username, String firstName, String lastName, boolean admin, long lastSeen) {
            this.userId = userId;
            this.username = username;
            this.firstName = firstName;
            this.lastName = lastName;
            this.admin = admin;
            this.lastSeen = lastSeen;
        }
    }

    public static class ProductCard {
        public final long productId;
        public final String channelId;
        public final long messageId;
        public final String mediaGroupId;
        public final String title;
        public final String size;
        public final String description;
        public final String article;
        public final double basePriceRsd;
        public final double currentPriceRsd;
        public final int discountPercent;
        public final Double fixedPriceRsd;
        public final String status;
        public final long createdAt;
        public final long updatedAt;

        public ProductCard(long productId,
                           String channelId,
                           long messageId,
                           String mediaGroupId,
                           String title,
                           String size,
                           String description,
                           String article,
                           double basePriceRsd,
                           double currentPriceRsd,
                           int discountPercent,
                           Double fixedPriceRsd,
                           String status,
                           long createdAt,
                           long updatedAt) {
            this.productId = productId;
            this.channelId = channelId;
            this.messageId = messageId;
            this.mediaGroupId = mediaGroupId;
            this.title = title;
            this.size = size;
            this.description = description;
            this.article = article;
            this.basePriceRsd = basePriceRsd;
            this.currentPriceRsd = currentPriceRsd;
            this.discountPercent = discountPercent;
            this.fixedPriceRsd = fixedPriceRsd;
            this.status = status;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }
    }
}
