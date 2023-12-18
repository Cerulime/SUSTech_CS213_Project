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
    private Timestamp lastUpdateTime;
    private final Transformer transformer;
    private final AsyncInitTable asyncInitTable;

    @Autowired
    public DatabaseServiceImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.lastUpdateTime = null;
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

        String config = """
                SET work_mem = '128MB';
                SET maintenance_work_mem = '1GB';
                SET effective_cache_size = '2GB';
                SET temp_buffers = '128MB';
                SET default_statistics_target = 500;
                SET max_parallel_workers_per_gather = 4;
                """;

        jdbcTemplate.execute(config);

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

        jdbcTemplate.execute("ANALYZE;");

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
                    WITH BvCount AS (
                        SELECT FLOOR(dis_time / 10) AS chunk, COUNT(*) AS count
                        FROM Danmu
                        WHERE bv = bv_value
                        GROUP BY FLOOR(dis_time / 10)
                    )
                    SELECT INTO max_count MAX(count) FROM BvCount;
                    
                    RETURN QUERY
                    SELECT chunk AS hotspot
                    FROM BvCount
                    WHERE count = max_count;
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
        String sql = "SELECT mid FROM UserAuth WHERE mid = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, mid) == null;
        } catch (EmptyResultDataAccessException e) {
            return true;
        }
    }

    @Override
    public boolean isQQorWechatExist(String qq, String wechat) {
        String sql = "SELECT mid FROM UserAuth WHERE qq = ? OR wechat = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, qq, wechat) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    public boolean isNameExist(String name) {
        String sql = "SELECT mid FROM UserProfile WHERE name = ?";
        try {
            return jdbcTemplate.queryForObject(sql, Long.class, escape(name)) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
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
        Pattern pattern = Pattern.compile("(\\d{1,2})月(\\d{1,2})日");
        String birthday = req.getBirthday();
        Matcher matcher = pattern.matcher(birthday);
        String birthday_month = birthday.isEmpty() ? null : matcher.group(1);
        String birthday_day = birthday.isEmpty() ? null : matcher.group(2);
        String escapeSign = escape(req.getSign());
        if (escapeSign.length() > MAX_SIGN_LENGTH) {
            log.info("Mid: {}", mid);
            log.info("Sign's length: {}", escapeSign.length());
            log.error("Sign is too long: {}", req.getSign());
            throw new IllegalArgumentException("Sign is too long");
        }
        sql = "INSERT INTO UserProfile(mid, name, sex, birthday_month, birthday_day, level, coin, sign, identity) VALUES (?, ?, ?, ?, 1, 0, ?, ?::Identity)";
        jdbcTemplate.update(sql, mid, escapeName, req.getSex(), birthday_month, birthday_day, escapeSign, UserRecord.Identity.USER.name());
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
        String sql = "SELECT follower FROM UserFollow WHERE follower = ? AND followee = ?";
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

    @Async
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

    @Async
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

    @Async
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

    @Async
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

    @Async
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

    @Async
    @Override
    public CompletableFuture<String[]> getPostedAsync(long mid) {
        return CompletableFuture.supplyAsync(() -> getPosted(mid));
    }

    @Override
    public String[] getPosted(long mid) {
        String sql = "SELECT bv FROM Video WHERE owner = ?";
        List<String> bvList = jdbcTemplate.queryForList(sql, String.class, mid);
        String[] bv = new String[bvList.size()];
        bvList.toArray(bv);
        return bv;
    }

    @Override
    public float getValidVideoDuration(String bv) {
        String sql = "SELECT duration FROM Video WHERE bv = ? AND (public_time IS NULL OR public_time < LOCALTIMESTAMP)";
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, Float.class, bv)).orElse(-1f);
        } catch (EmptyResultDataAccessException e) {
            return -1f;
        }
    }

    @Override
    public boolean isVideoUnwatched(long mid, String bv) {
        String sql = "SELECT mid FROM ViewVideo WHERE mid = ? AND bv = ?";
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
        String sql = "SELECT mid FROM LikeDanmu WHERE mid = ? AND id = ?";
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
        String sql = "SELECT owner FROM Video WHERE bv = ?";
        long ownerMid = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
        if (ownerMid < 0 || ownerMid == auth.getMid())
            return true;
        UserRecord.Identity identity = getUserIdentity(auth.getMid());
        if (identity == UserRecord.Identity.SUPERUSER)
            return false;
        sql = "SELECT reviewer FROM Video WHERE bv = ? AND (public_time IS NULL OR public_time < LOCALTIMESTAMP)";
        long reviewer = Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
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
        String sql = "SELECT mid FROM LikeVideo WHERE mid = ? AND bv = ?";
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
        String sql = "SELECT mid FROM FavVideo WHERE mid = ? AND bv = ?";
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
        String sql = "SELECT owner FROM Video WHERE bv = ?";
        return Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L);
    }

    @Override
    public boolean isVideoReviewed(String bv) {
        String sql = "SELECT reviewer FROM Video WHERE bv = ?";
        return Optional.ofNullable(jdbcTemplate.queryForObject(sql, Long.class, bv)).orElse(-1L) > 0;
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
            return jdbcTemplate.queryForObject(sql, Long.class, bv) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        String sql = "SELECT hotspot FROM get_hotspot(?)";
        return new HashSet<>(jdbcTemplate.queryForList(sql, Integer.class, bv));
    }

    @Override
    public double getVideoViewTime(String bv) {
        String sql = "SELECT SUM(view_time::DOUBLE PRECISION) FROM ViewVideo WHERE bv = ?";
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, Double.class, bv)).orElse(-1d);
        } catch (EmptyResultDataAccessException e) {
            return -1d;
        }
    }

    @Override
    public boolean isSameVideoExist(long mid, String title) {
        String escapeTitle = escape(title);
        String sql = "SELECT bv FROM Video WHERE owner = ? AND title = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, mid, escapeTitle) != null;
        } catch (EmptyResultDataAccessException e) {
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
        String sql = "INSERT INTO Video(bv, title, owner, commit_time, duration, description) VALUES (?, ?, ?, LOCALTIMESTAMP, ?, ?)";
        String bv = transformer.generateBV();
        jdbcTemplate.update(sql, bv, escapeTitle, mid, req.getDuration(), escapeDescription);
        return bv;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean deleteVideo(String bv) {
        String sql = "DELETE FROM Video WHERE bv = ?";
        return jdbcTemplate.update(sql, bv) > 0;
    }

    @Override
    public boolean isNewInfoValid(String bv, PostVideoReq req) {
        String sql = "SELECT title, duration, description, public_time FROM Video WHERE bv = ?";
        PostVideoReq origin = jdbcTemplate.queryForObject(sql, (rs, rowNum) -> PostVideoReq.builder()
                .title(rs.getString("title"))
                .duration(rs.getFloat("duration"))
                .description(rs.getString("description"))
                .publicTime(rs.getTimestamp("public_time"))
                .build(), bv);
        PostVideoReq escapeReq = PostVideoReq.builder()
                .title(escape(req.getTitle()))
                .duration(req.getDuration())
                .description(escape(req.getDescription()))
                .publicTime(req.getPublicTime()).build();
        assert origin != null;
        return !origin.equals(escapeReq) && origin.getDuration() == req.getDuration();
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
    public void createUnloggedTable(long mid) {
        String createPublicVideoTableConstrain = null;
        if (lastUpdateTime == null)
            createPublicVideoTableConstrain = """
                    ALTER TABLE PublicVideo(
                    ADD PRIMARY KEY (bv),
                    ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                    ON DELETE CASCADE
                    );
                    CREATE INDEX PublicVideoIndex ON PublicVideo (relevance DESC, view_count DESC);
                    """;
        String createPublicVideoTable = """
                CREATE UNLOGGED TABLE IF NOT EXISTS PublicVideo AS
                SELECT Video.bv AS bv, lower(CONCAT(Video.title, Video.description, UserProfile.name)) AS text, CountVideo.view_count AS view_count, 0 AS relevance
                FROM Video
                JOIN UserProfile ON Video.owner = UserProfile.mid
                JOIN CountVideo ON Video.bv = CountVideo.bv
                                
                """;
        String condition = """
                WHERE Video.owner = ? OR (Video.public_time IS NULL OR Video.public_time < LOCALTIMESTAMP)
                """;
        String returnTimestamp = "RETURNING LOCALTIMESTAMP;";
        UserRecord.Identity identity = getUserIdentity(mid);
        switch (identity) {
            case USER:
                lastUpdateTime = jdbcTemplate.queryForObject(createPublicVideoTable + condition + returnTimestamp, Timestamp.class, mid);
                break;
            case SUPERUSER:
                lastUpdateTime = jdbcTemplate.queryForObject(createPublicVideoTable + returnTimestamp, Timestamp.class);
                break;
        }
        if (createPublicVideoTableConstrain != null)
            jdbcTemplate.execute(createPublicVideoTableConstrain);
    }

    @SuppressWarnings("DuplicatedCode")
    @Override
    public void resetUnloggedTable(long mid) {
        String truncatePublicVideoTable = """
                TRUNCATE TABLE PublicVideo;
                """;
        jdbcTemplate.update(truncatePublicVideoTable);
        String insertPublicVideoTable = """
                INSERT INTO PublicVideo
                SELECT Video.bv AS bv, lower(CONCAT(Video.title, Video.description, UserProfile.name)) AS text, CountVideo.view_count AS view_count, 0 AS relevance
                FROM Video
                JOIN UserProfile ON Video.owner = UserProfile.mid
                JOIN CountVideo ON Video.bv = CountVideo.bv
                                
                """;
        String condition = """
                WHERE Video.owner = ? OR (Video.public_time IS NULL OR Video.public_time < LOCALTIMESTAMP)
                """;
        String returnTimestamp = "RETURNING LOCALTIMESTAMP;";
        UserRecord.Identity identity = getUserIdentity(mid);
        switch (identity) {
            case USER:
                lastUpdateTime = jdbcTemplate.queryForObject(insertPublicVideoTable + condition + returnTimestamp, Timestamp.class, mid);
                break;
            case SUPERUSER:
                lastUpdateTime = jdbcTemplate.queryForObject(insertPublicVideoTable + returnTimestamp, Timestamp.class);
                break;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void updateUnloggedTable(long mid) {
        String insertPublicVideoTable = """
                INSERT INTO PublicVideo
                SELECT Video.bv AS bv, lower(CONCAT(Video.title, Video.description, UserProfile.name)) AS text, 0 AS view_count, 0 AS relevance
                FROM UserProfile JOIN Video ON Video.owner = UserProfile.mid
                                
                """;
        String condition = """
                WHERE Video.owner = ? OR (Video.public_time IS NULL OR Video.public_time IN RANGE (?, LOCALTIMESTAMP)
                """;
        String returnTimestamp = "RETURNING LOCALTIMESTAMP;";
        UserRecord.Identity identity = getUserIdentity(mid);
        switch (identity) {
            case USER:
                lastUpdateTime = jdbcTemplate.queryForObject(insertPublicVideoTable + condition + returnTimestamp, Timestamp.class, lastUpdateTime, mid);
                break;
            case SUPERUSER:
                lastUpdateTime = jdbcTemplate.queryForObject(insertPublicVideoTable + returnTimestamp, Timestamp.class);
                break;
        }
        String updatePublicVideoTable = """
                UPDATE PublicVideo
                SET view_count = CountVideo.view_count
                FROM CountVideo
                WHERE PublicVideo.bv = CountVideo.bv;
                """;
        jdbcTemplate.update(updatePublicVideoTable);
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void updateRelevance(String s) {
        String updateRelevance = """
                UPDATE PublicVideo
                SET relevance = relevance + (length(text) - length(replace(text, ?, ''))) / length(?);
                """;
        jdbcTemplate.update(updateRelevance, s, s, s);
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
                OFFSET ?;
                """;
        return jdbcTemplate.queryForList(sql, String.class, pageSize, pageSize * (pageNum - 1));
    }

    @Override
    public List<String> getTopVideos(String bv) {
        String sql = """
                SELECT bv, COUNT(*) AS view_count
                FROM ViewVideo
                WHERE mid IN (
                    SELECT mid
                    FROM ViewVideo
                    WHERE bv = ?
                )
                AND bv <> ?
                GROUP BY bv
                ORDER BY view_count DESC
                LIMIT 5;
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("bv"), bv, bv);
    }

    @Override
    public List<String> getRecVideos(int pageSize, int pageNum) {
        String sql = """
                SELECT bv
                FROM CountVideo
                ORDER BY score DESC, view_count DESC
                LIMIT ?
                OFFSET ?;
                """;
        return jdbcTemplate.queryForList(sql, String.class, pageSize, pageSize * (pageNum - 1));
    }

    @Override
    public List<String> getRecVideosForUser(long mid, int pageSize, int pageNum) {
        String sql = """
                SELECT
                    vv.bv,
                    COUNT(vv.mid) AS view_count,
                    v.owner,
                    v.public_time,
                    up.level
                FROM (
                    SELECT uf1.followee AS mid
                    FROM UserFollow uf1
                    JOIN UserFollow uf2
                        ON uf1.follower = uf2.followee
                        AND uf1.followee = uf2.follower
                    WHERE uf1.follower = ?
                ) AS friends
                JOIN
                    ViewVideo vv ON friends.mid = vv.mid
                JOIN
                    Video v ON v.bv = vv.bv
                JOIN
                    UserProfile up ON v.owner = up.mid
                LEFT JOIN (
                    SELECT bv
                    FROM ViewVideo
                    WHERE mid = ?
                ) AS excluded_videos ON v.bv = excluded_videos.bv
                WHERE
                    excluded_videos.bv IS NULL AND (v.public_time IS NULL OR v.public_time < LOCALTIMESTAMP)
                GROUP BY
                    vv.bv
                ORDER BY
                    view_count DESC,
                    up.level DESC,
                    v.public_time DESC
                LIMIT ?
                OFFSET ?;
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("bv"), mid, mid, pageSize, pageSize * (pageNum - 1));
    }

    @Override
    public List<Long> getRecFriends(long mid, int pageSize, int pageNum) {
        String sql = """
                WITH user_followings AS (
                    SELECT followee
                    FROM UserFollow
                    WHERE follower = ?
                )
                                
                SELECT
                    uf.followee AS mid,
                    COUNT(uf.followee) AS common_followings,
                    up.level
                FROM
                    UserFollow uf
                JOIN
                    user_followings uf1 ON uf.follower = uf1.followee
                LEFT JOIN
                    user_followings uf2 ON uf.followee = uf2.followee
                JOIN
                    UserProfile up ON uf.followee = up.mid
                WHERE
                    uf2.followee IS NULL
                GROUP BY
                    uf.followee
                ORDER BY
                    common_followings DESC,
                    up.level DESC
                LIMIT ?
                OFFSET ?;
                """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("mid"), mid, pageSize, pageSize * (pageNum - 1));
    }

}
