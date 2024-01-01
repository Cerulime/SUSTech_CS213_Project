package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    private final JdbcTemplate jdbcTemplate;
    private final Transformer transformer;
    private final AsyncInitTable asyncInitTable;

    @Autowired
    public DatabaseServiceImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.transformer = new Transformer();
        this.asyncInitTable = new AsyncInitTable(jdbcTemplate);
    }

    private String escape(String input) {
        if (input == null)
            return "";
        return input.replace("\t", "\\t")
                .replace("\n", "\\n");
    }

    @Override
    public List<Integer> getGroupMembers() {
        return List.of(12212224);
    }

    private void setConfig() {
        String config = """
                SET work_mem = '256MB';
                SET maintenance_work_mem = '1.5GB';
                SET effective_cache_size = '4GB';
                SET temp_buffers = '256MB';
                SET default_statistics_target = 500;
                SET max_parallel_workers_per_gather = 8;
                """;
        jdbcTemplate.execute(config);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        log.info("Danmu record's size: " + danmuRecords.size());
        log.info("User record's size: " + userRecords.size());
        log.info("Video record's size: " + videoRecords.size());
        log.info("Begin importing at " + new Timestamp(new Date().getTime()));

        String createIdentityEnum = """
                DO $$
                  BEGIN
                      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'identity') THEN
                          CREATE TYPE Identity AS ENUM ('USER', 'SUPERUSER');
                      END IF;
                  END
                  $$;
                """;
        jdbcTemplate.execute(createIdentityEnum);
        String createGenderEnum = """
                DO $$
                  BEGIN
                      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'gender') THEN
                          CREATE TYPE Gender AS ENUM ('MALE', 'FEMALE', 'UNKNOWN');
                      END IF;
                  END
                  $$;
                """;
        jdbcTemplate.execute(createGenderEnum);

        setConfig();

        CompletableFuture<Void> future1 = asyncInitTable.initUserAuthTableAsync(userRecords);
        CompletableFuture<Void> future2 = future1.thenComposeAsync(aVoid -> CompletableFuture.allOf(
                asyncInitTable.initUserProfileTableAsync(userRecords),
                asyncInitTable.initUserFollowTableAsync(userRecords)
        ));
        CompletableFuture<Void> future3 = future1.thenComposeAsync(aVoid -> CompletableFuture.allOf(
                asyncInitTable.initVideoTableAsync(videoRecords)
        ));
        CompletableFuture<Void> future4 = future3.thenComposeAsync(aVoid -> CompletableFuture.allOf(
                asyncInitTable.initCountVideoTableAsync(videoRecords, danmuRecords),
                asyncInitTable.initViewVideoTableAsync(videoRecords),
                asyncInitTable.initLikeVideoTableAsync(videoRecords),
                asyncInitTable.initFavVideoTableAsync(videoRecords),
                asyncInitTable.initCoinVideoTableAsync(videoRecords)
        ));
        CompletableFuture<Void> future5 = future3.thenComposeAsync(aVoid -> CompletableFuture.allOf(
                asyncInitTable.initDanmuTableAsync(danmuRecords)
        ));
        CompletableFuture<Void> future6 = future5.thenComposeAsync(aVoid ->
                asyncInitTable.initLikeDanmuTableAsync(danmuRecords)
        );
        future2.join();
        future4.join();
        future6.join();

        createGetHotspotFunction();

        String createPublicVideoTable = String.format("""
                CREATE UNLOGGED TABLE IF NOT EXISTS PublicVideo (
                    bv CHAR(%d) PRIMARY KEY,
                    text VARCHAR(%d) NOT NULL,
                    view_count INTEGER NOT NULL,
                    relevance INTEGER NOT NULL,
                    FOREIGN KEY (bv) REFERENCES Video(bv) ON DELETE CASCADE
                );
                CREATE INDEX PublicVideoIndex ON PublicVideo (relevance DESC, view_count DESC);
                """, MAX_BV_LENGTH, MAX_TITLE_LENGTH + MAX_DESCRIPTION_LENGTH + MAX_NAME_LENGTH);
        jdbcTemplate.execute(createPublicVideoTable);

        log.info("End importing at " + new Timestamp(new Date().getTime()));

        transformer.setAvCount(asyncInitTable.getAvCount());
    }

    private void createGetHotspotFunction() {
        String createGetHotspotFunction = """
                CREATE OR REPLACE FUNCTION get_hotspot(bv_value CHAR(${MAX_BV_LENGTH}))
                RETURNS TABLE (hotspot INT) AS $$
                DECLARE
                    max_count INT;
                BEGIN
                    RETURN QUERY
                    WITH BvCount AS (
                        SELECT FLOOR(dis_time / 10)::integer AS chunk, COUNT(id) AS count
                        FROM Danmu
                        WHERE bv = bv_value
                        GROUP BY chunk
                    )
                    SELECT chunk AS hotspot
                    FROM BvCount
                    WHERE count = (SELECT MAX(count) FROM BvCount);
                END;
                $$ LANGUAGE plpgsql;
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH));
        jdbcTemplate.execute(createGetHotspotFunction);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void truncate() {
        if (ALLOW_TRUNCATE)
            truncating();
        else
            log.info("Do not truncate.");
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void truncating() {
        String sql = """
                DO $$
                DECLARE
                    tables CURSOR FOR
                        SELECT tablename
                        FROM pg_tables
                        WHERE schemaname = 'public';
                    views CURSOR FOR
                        SELECT matviewname
                        FROM pg_matviews
                        WHERE schemaname = 'public';
                BEGIN
                    FOR t IN tables
                    LOOP
                        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';
                    END LOOP;
                    FOR v IN views
                    LOOP
                        EXECUTE 'TRUNCATE MATERIALIZED VIEW ' || QUOTE_IDENT(v.matviewname) || ' CASCADE;';
                    END LOOP;
                END $$;
                """;
        jdbcTemplate.execute(sql);
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";
        return jdbcTemplate.queryForObject(sql, Integer.class, a, b);
    }

    @Override
    public AuthInfo getAuthInfo(long mid) {
        String sql = "SELECT password, qq, wechat FROM UserAuth WHERE mid = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> AuthInfo.builder()
                    .mid(mid)
                    .password(rs.getString("password"))
                    .qq(rs.getString("qq"))
                    .wechat(rs.getString("wechat"))
                    .build(), mid);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public AuthInfo getAuthInfoByQq(String qq) {
        String sql = "SELECT mid, password, wechat FROM UserAuth WHERE qq = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> AuthInfo.builder()
                    .mid(rs.getLong("mid"))
                    .password(rs.getString("password"))
                    .qq(qq)
                    .wechat(rs.getString("wechat"))
                    .build(), qq);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public AuthInfo getAuthInfoByWechat(String wechat) {
        String sql = "SELECT mid, password, qq FROM UserAuth WHERE wechat = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> AuthInfo.builder()
                    .mid(rs.getLong("mid"))
                    .password(rs.getString("password"))
                    .qq(rs.getString("qq"))
                    .wechat(wechat)
                    .build(), wechat);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public boolean isMidNotExist(long mid) {
        String sql = "SELECT 1 FROM UserAuth WHERE mid = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, mid) == null;
        } catch (EmptyResultDataAccessException e) {
            return true;
        }
    }

    @Override
    public boolean isQQorWechatExist(String qq, String wechat) {
        String sql = "SELECT 1 FROM UserAuth WHERE qq = ? OR wechat = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, qq, wechat) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    public boolean isNameExist(String name) {
        String sql = "SELECT 1 FROM UserProfile WHERE name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, escape(name)) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    private Short parseShort(String s) {
        try {
            return Short.parseShort(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public long insertUser(RegisterUserReq req) {
        String sql = "INSERT INTO UserAuth(password, qq, wechat) VALUES (?, ?, ?) RETURNING mid";
        Long mid = jdbcTemplate.queryForObject(sql, Long.class,
                UserService.encodePassword(req.getPassword()),
                req.getQq(),
                req.getWechat());
        if (mid == null)
            return -1;
        String escapeName = escape(req.getName());
        if (escapeName.length() > MAX_NAME_LENGTH) {
            log.info("Mid: {}", mid);
            log.info("Name's length: {}", escapeName.length());
            log.error("Name is too long: {}", req.getName());
            throw new IllegalArgumentException("Name is too long");
        }
        String birthday = req.getBirthday();
        String birthday_month = null;
        String birthday_day = null;
        if (birthday != null && !birthday.isEmpty()) {
            Pattern pattern = Pattern.compile("(\\d{1,2})月(\\d{1,2})日");
            Matcher matcher = pattern.matcher(birthday);
            if (!matcher.matches()) {
                Pattern pattern2 = Pattern.compile("(\\d{1,2})-(\\d{1,2})");
                Matcher matcher2 = pattern2.matcher(birthday);
                if (!matcher2.matches()) {
                    log.info("Mid: {}", mid);
                    log.error("Invalid birthday: {}", birthday);
                    throw new IllegalArgumentException("Invalid birthday");
                }
                birthday_month = matcher2.group(1);
                birthday_day = matcher2.group(2);
            } else {
                birthday_month = matcher.group(1);
                birthday_day = matcher.group(2);
            }
        }
        String escapeSign = escape(req.getSign());
        if (escapeSign.length() > MAX_SIGN_LENGTH) {
            log.info("Mid: {}", mid);
            log.info("Sign's length: {}", escapeSign.length());
            log.error("Sign is too long: {}", req.getSign());
            throw new IllegalArgumentException("Sign is too long");
        }
        sql = "INSERT INTO UserProfile(mid, name, sex, birthday_month, birthday_day, level, coin, sign, identity) VALUES (?, ?, ?::Gender, ?, ?, 1, 0, ?, ?::Identity)";
        jdbcTemplate.update(sql, mid, escapeName, req.getSex().name(), parseShort(birthday_month), parseShort(birthday_day), escapeSign, UserRecord.Identity.USER.name());
        return mid;
    }

    @Override
    public UserRecord.Identity getUserIdentity(long mid) {
        String sql = "SELECT identity FROM UserProfile WHERE mid = ?";
        try {
            return jdbcTemplate.queryForObject(sql, UserRecord.Identity.class, mid);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean deleteUser(long mid) {
        String sql = "DELETE FROM UserAuth WHERE mid = ?";
        return jdbcTemplate.update(sql, mid) > 0;
    }

    @Override
    public boolean isFollowing(long followerMid, long followeeMid) {
        String sql = "SELECT 1 FROM UserFollow WHERE follower = ? AND followee = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, followerMid, followeeMid) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean follow(long followerMid, long followeeMid) {
        String sql = "INSERT INTO UserFollow(follower, followee) VALUES (?, ?)";
        return jdbcTemplate.update(sql, followerMid, followeeMid) > 0;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean unfollow(long followerMid, long followeeMid) {
        String sql = "DELETE FROM UserFollow WHERE follower = ? AND followee = ?";
        return jdbcTemplate.update(sql, followerMid, followeeMid) > 0;
    }

    @Override
    public int getCoin(long mid) {
        String sql = "SELECT coin FROM UserProfile WHERE mid = ?";
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, Integer.class, mid)).orElse(-1);
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<long[]> getFollowingAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getFollowing(mid));
    }

    @Override
    public long[] getFollowing(long mid) {
        String sql = "SELECT followee FROM UserFollow WHERE follower = ?";
        List<Long> followeeList = jdbcTemplate.queryForList(sql, Long.class, mid);
        return followeeList.stream().mapToLong(Long::longValue).toArray();
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<long[]> getFollowerAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getFollower(mid));
    }

    @Override
    public long[] getFollower(long mid) {
        String sql = "SELECT follower FROM UserFollow WHERE followee = ?";
        List<Long> followerList = jdbcTemplate.queryForList(sql, Long.class, mid);
        return followerList.stream().mapToLong(Long::longValue).toArray();
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<String[]> getWatchedAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getWatched(mid));
    }

    @Override
    public String[] getWatched(long mid) {
        String sql = "SELECT bv FROM ViewVideo WHERE mid = ?";
        List<String> bvList = jdbcTemplate.queryForList(sql, String.class, mid);
        String[] bv = new String[bvList.size()];
        bvList.toArray(bv);
        return bv;
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<String[]> getLikedAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getLiked(mid));
    }

    @Override
    public String[] getLiked(long mid) {
        String sql = "SELECT bv FROM LikeVideo WHERE mid = ?";
        List<String> bvList = jdbcTemplate.queryForList(sql, String.class, mid);
        String[] bv = new String[bvList.size()];
        bvList.toArray(bv);
        return bv;
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<String[]> getCollectedAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getCollected(mid));
    }

    @Override
    public String[] getCollected(long mid) {
        String sql = "SELECT bv FROM FavVideo WHERE mid = ?";
        List<String> bvList = jdbcTemplate.queryForList(sql, String.class, mid);
        String[] bv = new String[bvList.size()];
        bvList.toArray(bv);
        return bv;
    }

    @Async("taskExecutor")
    @Override
    public CompletableFuture<String[]> getPostedAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getPosted(mid));
    }

    @Override
    public String[] getPosted(long mid) {
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT bv FROM Video WHERE owner = ?";
        List<String> bvList = jdbcTemplate.queryForList(sql, String.class, mid);
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        jdbcTemplate.execute(enableSeqScan);
        String[] bv = new String[bvList.size()];
        bvList.toArray(bv);
        return bv;
    }

    @Override
    public float getValidVideoDuration(String bv) {
        if (bv == null || bv.isEmpty())
            return -1;
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT duration FROM Video WHERE bv = ? AND public_time < LOCALTIMESTAMP";
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        try {
            float res = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Float.class, bv)).orElse(-1f);
            jdbcTemplate.execute(enableSeqScan);
            return res;
        } catch (EmptyResultDataAccessException e) {
            jdbcTemplate.execute(enableSeqScan);
            return -1f;
        }
    }

    @Override
    public boolean isVideoUnwatched(long mid, String bv) {
        String sql = "SELECT 1 FROM ViewVideo WHERE mid = ? AND bv = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, mid, bv) == null;
        } catch (EmptyResultDataAccessException e) {
            return true;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public long insertDanmu(long mid, String bv, String content, float time) {
        String sql = "INSERT INTO Danmu(bv, mid, dis_time, content, post_time) VALUES (?, ?, ?, ?, LOCALTIMESTAMP) RETURNING id";
        Long id = jdbcTemplate.queryForObject(sql, Long.class, bv, mid, time, content);
        if (id == null)
            return -1;
        return id;
    }

    @Override
    public List<Long> getDanmu(String bv, float timeStart, float timeEnd) {
        String sql = "SELECT id FROM Danmu WHERE bv = ? AND dis_time BETWEEN ? AND ?";
        return jdbcTemplate.queryForList(sql, Long.class, bv, timeStart, timeEnd);
    }

    @Override
    public List<Long> getDanmuFiltered(String bv, float timeStart, float timeEnd) {
        setConfig();
        String sql = """
                SELECT DISTINCT ON (content) id
                FROM Danmu WHERE bv = ? AND dis_time BETWEEN ? AND ?
                ORDER BY content, post_time ASC
                """;
        return jdbcTemplate.queryForList(sql, Long.class, bv, timeStart, timeEnd);
    }

    @Override
    public String getBvByDanmuId(long id) {
        String sql = "SELECT bv FROM Danmu WHERE id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public boolean isDanmuLiked(long mid, long id) {
        String sql = "SELECT 1 FROM LikeDanmu WHERE mid = ? AND id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, mid, id) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean unlikeDanmu(long mid, long id) {
        String sql = "DELETE FROM LikeDanmu WHERE mid = ? AND id = ?";
        return jdbcTemplate.update(sql, mid, id) > 0;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean likeDanmu(long mid, long id) {
        String sql = "INSERT INTO LikeDanmu(mid, id) VALUES (?, ?)";
        return jdbcTemplate.update(sql, mid, id) > 0;
    }

    @Override
    public boolean isVideoNotEngage(AuthInfo auth, String bv) {
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        String sql = "SELECT owner FROM Video WHERE bv = ?";
        long ownerMid;
        try {
            ownerMid = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
        } catch (EmptyResultDataAccessException e) {
            ownerMid = -1;
        }
        if (ownerMid < 0 || ownerMid == auth.getMid()) {
            jdbcTemplate.execute(enableSeqScan);
            return true;
        }
        UserRecord.Identity identity = getUserIdentity(auth.getMid());
        if (identity == UserRecord.Identity.SUPERUSER) {
            jdbcTemplate.execute(enableSeqScan);
            return false;
        }
        sql = "SELECT reviewer FROM Video WHERE bv = ? AND public_time < LOCALTIMESTAMP";
        long reviewer;
        try {
            reviewer = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
        } catch (EmptyResultDataAccessException e) {
            reviewer = -1;
        }
        jdbcTemplate.execute(enableSeqScan);
        return reviewer <= 0;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean coinVideo(long mid, String bv) {
        String sql = "INSERT INTO CoinVideo(mid, bv) VALUES (?, ?)";
        try {
            return jdbcTemplate.update(sql, mid, bv) > 0;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void updateCoin(long mid, int newCoin) {
        String sql = "UPDATE UserProfile SET coin = ? WHERE mid = ?";
        jdbcTemplate.update(sql, newCoin, mid);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean likeVideo(long mid, String bv) {
        String sql = "INSERT INTO LikeVideo(mid, bv) VALUES (?, ?)";
        return jdbcTemplate.update(sql, mid, bv) > 0;
    }

    @Override
    public boolean isVideoLiked(long mid, String bv) {
        String sql = "SELECT 1 FROM LikeVideo WHERE mid = ? AND bv = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, mid, bv) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean unlikeVideo(long mid, String bv) {
        String sql = "DELETE FROM LikeVideo WHERE mid = ? AND bv = ?";
        return jdbcTemplate.update(sql, mid, bv) > 0;
    }

    @Override
    public boolean isVideoCollected(long mid, String bv) {
        String sql = "SELECT 1 FROM FavVideo WHERE mid = ? AND bv = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, mid, bv) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean uncollectVideo(long mid, String bv) {
        String sql = "DELETE FROM FavVideo WHERE mid = ? AND bv = ?";
        return jdbcTemplate.update(sql, mid, bv) > 0;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean collectVideo(long mid, String bv) {
        String sql = "INSERT INTO FavVideo(mid, bv) VALUES (?, ?)";
        return jdbcTemplate.update(sql, mid, bv) > 0;
    }

    @Override
    public long getVideoOwner(String bv) {
        if (bv == null || bv.isEmpty())
            return -1;
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT owner FROM Video WHERE bv = ?";
        long owner;
        try {
            owner = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
        } catch (EmptyResultDataAccessException e) {
            owner = -1;
        }
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        jdbcTemplate.execute(enableSeqScan);
        return owner;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public boolean isVideoReviewed(String bv) {
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT reviewer FROM Video WHERE bv = ?";
        long reviewer;
        try {
            reviewer = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
        } catch (EmptyResultDataAccessException e) {
            reviewer = -1;
        }
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        jdbcTemplate.execute(enableSeqScan);
        return reviewer > 0;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean reviewVideo(long mid, String bv) {
        String sql = "UPDATE Video SET reviewer = ?, review_time = LOCALTIMESTAMP WHERE bv = ?";
        return jdbcTemplate.update(sql, mid, bv) > 0;
    }

    @Override
    public boolean isDanmuExistByBv(String bv) {
        String sql = "SELECT EXISTS(SELECT id FROM Danmu WHERE bv = ?)";
        try {
            return Boolean.TRUE.equals(jdbcTemplate.queryForObject(sql, Boolean.class, bv));
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        setConfig();
        String sql = "SELECT hotspot FROM get_hotspot(?)";
        return new HashSet<>(jdbcTemplate.queryForList(sql, Integer.class, bv));
    }

    @Override
    public double getAverageViewRate(String bv) {
        String sql = "SELECT view_count, view_rate FROM CountVideo WHERE bv = ?";
        int view_count;
        double view_rate;
        try {
            Map<String, Object> map = jdbcTemplate.queryForMap(sql, bv);
            view_count = (int) map.get("view_count");
            view_rate = (double) map.get("view_rate");
        } catch (EmptyResultDataAccessException e) {
            return -1;
        }
        if (view_count == 0)
            return -1;
        return view_rate / view_count;
    }

    @Override
    public boolean isSameVideoExist(long mid, String title) {
        String escapeTitle = escape(title);
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT 1 FROM Video WHERE owner = ? AND title = ?";
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        try {
            String res = jdbcTemplate.queryForObject(sql, String.class, mid, escapeTitle);
            jdbcTemplate.execute(enableSeqScan);
            return res != null;
        } catch (EmptyResultDataAccessException e) {
            jdbcTemplate.execute(enableSeqScan);
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public String insertVideo(long mid, PostVideoReq req) {
        String escapeTitle = escape(req.getTitle());
        if (escapeTitle.length() > MAX_TITLE_LENGTH) {
            log.info("Mid: {}", mid);
            log.info("Title's length: {}", escapeTitle.length());
            log.error("Title is too long: {}", req.getTitle());
            throw new IllegalArgumentException("Title is too long");
        }
        String escapeDescription = escape(req.getDescription());
        if (escapeDescription.length() > MAX_DESCRIPTION_LENGTH) {
            log.info("Mid: {}", mid);
            log.info("Description's length: {}", escapeDescription.length());
            log.error("Description is too long: {}", req.getDescription());
            throw new IllegalArgumentException("Description is too long");
        }
        String sql = "INSERT INTO Video(bv, title, owner, commit_time, duration, description, public_time) VALUES (?, ?, ?, LOCALTIMESTAMP, ?, ?, ?)";
        String bv = transformer.generateBV();
        jdbcTemplate.update(sql, bv, escapeTitle, mid, req.getDuration(), escapeDescription, req.getPublicTime());
        return bv;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean deleteVideo(String bv) {
        setConfig();
        String disableTrigger = """
                ALTER TABLE Danmu DISABLE TRIGGER delete_danmu_count;
                ALTER TABLE CountVideo DISABLE TRIGGER update_score;
                ALTER TABLE ViewVideo DISABLE TRIGGER delete_view_count;
                ALTER TABLE LikeVideo DISABLE TRIGGER delete_like_count;
                ALTER TABLE FavVideo DISABLE TRIGGER delete_fav_count;
                ALTER TABLE CoinVideo DISABLE TRIGGER delete_coin_count;
                """;
        jdbcTemplate.execute(disableTrigger);
        String sql = "DELETE FROM Video WHERE bv = ?";
        int res = jdbcTemplate.update(sql, bv);
        String enableTrigger = """
                ALTER TABLE Danmu ENABLE TRIGGER delete_danmu_count;
                ALTER TABLE CountVideo ENABLE TRIGGER update_score;
                ALTER TABLE ViewVideo ENABLE TRIGGER delete_view_count;
                ALTER TABLE LikeVideo ENABLE TRIGGER delete_like_count;
                ALTER TABLE FavVideo ENABLE TRIGGER delete_fav_count;
                ALTER TABLE CoinVideo ENABLE TRIGGER delete_coin_count;
                """;
        jdbcTemplate.execute(enableTrigger);
        return res > 0;
    }

    @Override
    public boolean isNewInfoValid(String bv, PostVideoReq req) {
        String disableSeqScan = """
                SET enable_seqscan = off;
                """;
        jdbcTemplate.execute(disableSeqScan);
        String sql = "SELECT title, duration, description, public_time FROM Video WHERE bv = ?";
        PostVideoReq origin = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> PostVideoReq.builder()
                .title(rs.getString("title"))
                .duration(rs.getFloat("duration"))
                .description(rs.getString("description"))
                .publicTime(rs.getTimestamp("public_time"))
                .build(), bv);
        String enableSeqScan = """
                SET enable_seqscan = on;
                """;
        jdbcTemplate.execute(enableSeqScan);
        PostVideoReq escapeReq = PostVideoReq.builder()
                .title(escape(req.getTitle()))
                .duration(req.getDuration())
                .description(escape(req.getDescription()))
                .publicTime(req.getPublicTime()).build();
        assert origin != null;
        return !origin.isSame(escapeReq) && Math.abs(origin.getDuration() - req.getDuration()) < EPSILON;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean updateVideoInfo(String bv, PostVideoReq req) {
        String sql = "UPDATE Video SET title = ?, duration = ?, description = ?, public_time = ?, reviewer = NULL, review_time = NULL WHERE bv = ?";
        return jdbcTemplate.update(sql, escape(req.getTitle()), req.getDuration(), escape(req.getDescription()), req.getPublicTime(), bv) > 0;
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void resetUnloggedTable(long mid) {
        setConfig();
        String updateViewCount = """
                UPDATE PublicVideo
                SET view_count = CountVideo.view_count, relevance = 0
                FROM CountVideo
                WHERE PublicVideo.bv = CountVideo.bv;
                """;
        String insertPublicVideoTable = """
                INSERT INTO PublicVideo (bv, text, view_count, relevance)
                SELECT Video.bv AS bv, lower(CONCAT(Video.title, Video.description, UserProfile.name)) AS text, CountVideo.view_count AS view_count, 0 AS relevance
                FROM Video
                LEFT JOIN PublicVideo ON Video.bv = PublicVideo.bv
                JOIN UserProfile ON Video.owner = UserProfile.mid
                JOIN CountVideo ON Video.bv = CountVideo.bv
                WHERE PublicVideo.bv IS NULL
                """;
        String condition = " AND (Video.owner = ? OR Video.public_time < LOCALTIMESTAMP)";
        UserRecord.Identity identity = getUserIdentity(mid);
        switch (identity) {
            case USER:
                String deleteInvalidVideo = """
                        DELETE FROM PublicVideo
                        WHERE bv IN (
                            SELECT pv.bv
                            FROM PublicVideo pv
                            LEFT JOIN Video v ON pv.bv = v.bv
                                AND (v.owner = ? OR v.public_time < LOCALTIMESTAMP)
                            WHERE v.bv IS NULL
                        )
                        """;
                jdbcTemplate.update(deleteInvalidVideo, mid);
                jdbcTemplate.update(updateViewCount);
                jdbcTemplate.update(insertPublicVideoTable + condition, mid);
                break;
            case SUPERUSER:
                jdbcTemplate.update(updateViewCount);
                jdbcTemplate.update(insertPublicVideoTable);
                break;
        }
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void createTempTable(long mid) {
        setConfig();
        String createTempTable = """
                CREATE TEMP TABLE TempVideo AS
                SELECT Video.bv AS bv, lower(CONCAT(Video.title, Video.description, UserProfile.name)) AS text, CountVideo.view_count AS view_count, 0 AS relevance
                FROM Video
                JOIN UserProfile ON Video.owner = UserProfile.mid
                JOIN CountVideo ON Video.bv = CountVideo.bv
                LEFT JOIN PublicVideo ON Video.bv = PublicVideo.bv
                WHERE PublicVideo.bv IS NULL
                """;
        String condition = " AND (Video.owner = ? OR Video.public_time < LOCALTIMESTAMP)";
        UserRecord.Identity identity = getUserIdentity(mid);
        switch (identity) {
            case USER:
                jdbcTemplate.update(createTempTable + condition, mid);
                break;
            case SUPERUSER:
                jdbcTemplate.update(createTempTable);
                break;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void updateRelevanceTemp(String s) {
        setConfig();
        String updateRelevanceTemp = """
                UPDATE TempVideo
                SET relevance = relevance + (length(text) - length(replace(text, ?, ''))) / length(?);
                """;
        jdbcTemplate.update(updateRelevanceTemp, s, s, s);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void mergeTemp(long mid) {
        setConfig();
        String updateViewCount = """
                UPDATE PublicVideo
                SET view_count = CountVideo.view_count
                FROM CountVideo
                WHERE PublicVideo.bv = CountVideo.bv;
                """;
        String mergeTemp = """
                INSERT INTO PublicVideo (bv, text, view_count, relevance)
                SELECT bv, text, view_count, relevance
                FROM TempVideo
                """;
        UserRecord.Identity identity = getUserIdentity(mid);
        if (identity == UserRecord.Identity.USER) {
            String deleteInvalidVideo = """
                    DELETE pv
                    FROM PublicVideo pv
                    LEFT JOIN Video v ON pv.bv = v.bv
                        AND (v.owner = ? OR v.public_time < LOCALTIMESTAMP)
                    WHERE v.bv IS NULL
                    """;
            jdbcTemplate.update(deleteInvalidVideo, mid);
        }
        jdbcTemplate.update(updateViewCount);
        jdbcTemplate.update(mergeTemp);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void updateRelevance(String s) {
        setConfig();
        String updateRelevance = """
                UPDATE PublicVideo
                SET relevance = relevance + (length(text) - length(replace(text, ?, ''))) / length(?)
                """;
        jdbcTemplate.update(updateRelevance, s, s);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public List<String> searchVideo(int pageSize, int pageNum) {
        String sql = """
                SELECT bv
                FROM PublicVideo
                WHERE relevance > 0
                ORDER BY relevance DESC, view_count DESC
                LIMIT ?
                OFFSET ?
                """;
        return jdbcTemplate.queryForList(sql, String.class, pageSize, pageSize * (pageNum - 1));
    }

    @Override
    public List<String> getTopVideos(String bv) {
        setConfig();
        String JoinPath = """
                SELECT v1.bv, Count(v1.bv) as bv_count
                FROM ViewVideo v1
                JOIN ViewVideo v2 ON v1.mid = v2.mid AND v2.bv = ?
                GROUP BY v1.bv
                ORDER BY bv_count DESC, v1.bv ASC
                LIMIT 5
                OFFSET 1
                """;
        return jdbcTemplate.query(JoinPath, (rs, rowNum) -> rs.getString("bv"), bv);
    }

    @Override
    public List<String> getRecVideos(int pageSize, int pageNum) {
        String sql = """
                SELECT bv
                FROM CountVideo
                ORDER BY score DESC, view_count DESC
                LIMIT ?
                OFFSET ?
                """;
        return jdbcTemplate.queryForList(sql, String.class, pageSize, pageSize * (pageNum - 1));
    }

    @Override
    public List<String> getRecVideosForUser(long mid, int pageSize, int pageNum) {
        setConfig();
        String sql = """
                SELECT vv.bv, COUNT(vv.mid) AS view_count, up.level, v.public_time
                FROM (
                    SELECT uf1.followee AS mid
                    FROM UserFollow uf1
                    JOIN UserFollow uf2
                        ON uf1.follower = uf2.followee
                        AND uf1.followee = uf2.follower
                    WHERE uf1.follower = ?
                ) AS friends
                JOIN ViewVideo vv ON friends.mid = vv.mid
                LEFT JOIN (
                    SELECT bv
                    FROM ViewVideo
                    WHERE mid = ?
                ) AS excluded_videos ON vv.bv = excluded_videos.bv
                JOIN Video v ON v.bv = vv.bv
                JOIN UserProfile up ON v.owner = up.mid
                WHERE
                    excluded_videos.bv IS NULL AND v.public_time < LOCALTIMESTAMP
                GROUP BY vv.bv, up.level, v.public_time
                HAVING COUNT(vv.mid) > 0
                ORDER BY
                    view_count DESC,
                    up.level DESC,
                    v.public_time DESC
                LIMIT ?
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("bv"), mid, mid, pageSize * pageNum);
    }

    @Override
    public List<Long> getRecFriends(long mid, int pageSize, int pageNum) {
        setConfig();
        String sql = """
                SELECT
                    uf.follower AS mid,
                    COUNT(uf.followee) AS common_followings,
                    up.level
                FROM UserFollow uf
                JOIN UserProfile up ON uf.follower = up.mid
                WHERE
                    uf.follower <> ?
                    AND uf.followee = ANY(ARRAY(SELECT followee FROM UserFollow WHERE follower = ?))
                    AND uf.follower NOT IN (SELECT followee FROM UserFollow WHERE follower = ?)
                GROUP BY uf.follower, up.level
                ORDER BY common_followings DESC, up.level DESC, mid ASC
                LIMIT ?
                OFFSET ?
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("mid"), mid, mid, mid, pageSize, pageSize * (pageNum - 1));
    }

}
