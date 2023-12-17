package io.sustc.service.impl;

import com.google.common.base.Joiner;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import com.google.common.io.CharSource;
import com.google.common.primitives.Floats;
import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
@EnableAsync
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    private final JdbcTemplate jdbcTemplate;
    private Timestamp lastUpdateTime;
    private final Escaper escaper = Escapers.builder()
            .addEscape('\n', "\\n")
            .addEscape('\t', "\\t")
            .build();
    private static final int[] bvState = {11, 10, 3, 8, 4, 6};
    private static final long bvXOR = 177451812L;
    private static final long bvAdd = 8728348608L;
    @SuppressWarnings("SpellCheckingInspection")
    private static final char[] trTable = "fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF".toCharArray();
    private static final int[] tr = new int[128];
    private static final long[] pow58 = new long[6];

    static {
        for (int i = 0; i < 58; i++) {
            tr[trTable[i]] = i;
        }
        pow58[0] = 1;
        for (int i = 1; i < 6; i++) {
            pow58[i] = pow58[i - 1] * 58;
        }
    }

    private long avCount = 10001;

    private long getAv(String bv) {
        long r = 0;
        for (int i = 0; i < 6; i++) {
            r += pow58[i] * tr[bv.charAt(bvState[i])];
        }
        return (r - bvAdd) ^ bvXOR;
    }

    private String getBv(long av) {
        av = (av ^ bvXOR) + bvAdd;
        char[] r = "BV1  4 1 7  ".toCharArray();
        for (int i = 0; i < 6; i++) {
            r[bvState[i]] = trTable[(int) (av / pow58[i] % 58)];
        }
        return new String(r);
    }

    private String generateBV() {
        avCount++;
        return getBv(avCount);
    }

    @Autowired
    public DatabaseServiceImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.lastUpdateTime = null;
    }

    @Override
    public List<Integer> getGroupMembers() {
        return List.of(12212224);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initUserAuthTable(List<UserRecord> userRecords) {
        String createUserAuthTable = String.format("""
                CREATE TABLE IF NOT EXISTS UserAuth (
                    mid BIGSERIAL,
                    password CHAR(%d),
                    qq VARCHAR(%d),
                    wechat VARCHAR(%d)
                );
                """, MAX_PASSWORD_LENGTH, MAX_QQ_LENGTH, MAX_WECHAT_LENGTH);
        jdbcTemplate.execute(createUserAuthTable);
        log.info("Begin encoding passwords");
        Joiner joiner = Joiner.on('\t').useForNull("");
        AtomicBoolean hasError = new AtomicBoolean(false);
        String copyData = userRecords.parallelStream().map(user -> {
            if (user.getQq() != null && user.getQq().length() > MAX_QQ_LENGTH) {
                log.error("QQ is too long: {}", user.getQq());
                hasError.set(true);
                return null;
            }
            if (user.getWechat() != null && user.getWechat().length() > MAX_WECHAT_LENGTH) {
                log.error("WeChat is too long: {}", user.getWechat());
                hasError.set(true);
                return null;
            }
            String encodedPassword = UserService.encodePassword(user.getPassword());
            return joiner.join(user.getMid(), encodedPassword, user.getQq(), user.getWechat());
        }).filter(s -> s != null && !hasError.get()).collect(Collectors.joining("\n"));
        if (hasError.get()) {
            throw new IllegalArgumentException("One or more records have errors. Check logs for details.");
        }
        log.info("Finish encoding passwords");
        String copySql = "COPY UserAuth(mid, password, qq, wechat) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', FREEZE)";
        copyInsertion(CharSource.wrap(copyData), copySql);
        String createUserAuthTableConstraint = """
                SELECT setval(pg_get_serial_sequence('UserAuth', 'mid'), (SELECT MAX(mid) FROM UserAuth));
                                
                ALTER TABLE UserAuth ALTER COLUMN password SET NOT NULL;
                                
                ALTER TABLE UserAuth ADD PRIMARY KEY (mid);
                ALTER TABLE UserAuth ADD CONSTRAINT UniqueQq UNIQUE (qq);
                ALTER TABLE UserAuth ADD CONSTRAINT UniqueWechat UNIQUE (wechat);
                                
                CREATE INDEX UserAuthQqWechatIndex ON UserAuth (qq, wechat);
                """;
        jdbcTemplate.execute(createUserAuthTableConstraint);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initUserProfileTable(List<UserRecord> userRecords) {
        String createUserProfileTable = String.format("""
                CREATE TABLE IF NOT EXISTS UserProfile(
                    mid BIGINT,
                    name VARCHAR(%d),
                    sex Gender,
                    birthday_month SMALLINT,
                    birthday_day SMALLINT,
                    level SMALLINT,
                    coin INTEGER,
                    sign VARCHAR(%d),
                    identity Identity
                );
                """, MAX_NAME_LENGTH, MAX_SIGN_LENGTH);
        jdbcTemplate.execute(createUserProfileTable);
        Joiner joiner = Joiner.on('\t').useForNull("");
        StringBuilder copyData = new StringBuilder();
        Pattern pattern = Pattern.compile("(\\d+)月(\\d+)日");
        for (UserRecord user : userRecords) {
            String birthday = user.getBirthday();
            Matcher matcher = pattern.matcher(birthday);
            boolean isEmpty = birthday.isEmpty();
            if (!isEmpty && !matcher.matches()) {
                log.info("User mid: {}", user.getMid());
                log.error("Invalid birthday: {}", user.getBirthday());
                throw new IllegalArgumentException("Invalid birthday");
            }
            if (escaper.escape(user.getName()).length() > MAX_NAME_LENGTH) {
                log.info("User mid: {}", user.getMid());
                log.error("Name is too long: {}", user.getName());
                throw new IllegalArgumentException("Name is too long");
            }
            String escapeSign = escaper.escape(user.getSign());
            if (escapeSign.length() > MAX_SIGN_LENGTH) {
                if (escapeSign.replace("\\n", "").length() < MAX_SIGN_LENGTH) {
                    escapeSign = escapeSign.replace("\\n", "");
                    log.info("User mid: {}", user.getMid());
                    log.warn("User has too sign with \\n: {}", user.getSign());
                } else {
                    log.info("User mid: {}", user.getMid());
                    log.error("Sign is too long: {}", user.getSign());
                    throw new IllegalArgumentException("Sign is too long");
                }
            }
            joiner.appendTo(copyData,
                    user.getMid(),
                    escaper.escape(user.getName()),
                    user.getSex(),
                    isEmpty ? null : matcher.group(1),
                    isEmpty ? null : matcher.group(2),
                    user.getLevel(),
                    user.getCoin(),
                    escapeSign,
                    user.getIdentity().name());
            copyData.append('\n');
        }
        String copySql = "COPY UserProfile(mid, name, sex, birthday_month, birthday_day, level, coin, sign, identity) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x07', FREEZE)";
        copyInsertion(CharSource.wrap(copyData), copySql);
        String createUserProfileTableConstraint = """
                ALTER TABLE UserProfile ALTER COLUMN name SET NOT NULL;
                ALTER TABLE UserProfile ALTER COLUMN level SET NOT NULL;
                ALTER TABLE UserProfile ALTER COLUMN coin SET NOT NULL;
                ALTER TABLE UserProfile ALTER COLUMN identity SET NOT NULL;
                                
                ALTER TABLE UserProfile ADD PRIMARY KEY (mid);
                ALTER TABLE UserProfile ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE UserProfile ADD CONSTRAINT UniqueName UNIQUE (name);
                                
                CREATE INDEX UserProfileNameIndex ON UserProfile USING HASH (name);
                CREATE INDEX UserProfileLevelIndex ON UserProfile(level DESC);
                """;
        jdbcTemplate.execute(createUserProfileTableConstraint);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initUserFollowTable(List<UserRecord> userRecords) {
        String createUserFollowTable = """
                CREATE TABLE IF NOT EXISTS UserFollow(
                    follower BIGINT,
                    followee BIGINT
                ) PARTITION BY HASH (follower);
                CREATE TABLE UserFollow_1 PARTITION OF UserFollow FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE UserFollow_2 PARTITION OF UserFollow FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE UserFollow_3 PARTITION OF UserFollow FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE UserFollow_4 PARTITION OF UserFollow FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createUserFollowTable);
        String copySql = "COPY UserFollow(follower, followee) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000;
        for (UserRecord user : userRecords) {
            for (long followee : user.getFollowing()) {
                joiner.appendTo(copyData,
                        user.getMid(),
                        followee
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createUserFollowTableConstraint = """
                ALTER TABLE UserFollow ALTER COLUMN follower SET NOT NULL;
                ALTER TABLE UserFollow ALTER COLUMN followee SET NOT NULL;
                                
                ALTER TABLE UserFollow ADD PRIMARY KEY (follower, followee);
                ALTER TABLE UserFollow ADD FOREIGN KEY (follower) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE UserFollow ADD FOREIGN KEY (followee) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                                
                CREATE INDEX UserFolloweeIndex ON UserFollow(followee);
                """;
        jdbcTemplate.execute(createUserFollowTableConstraint);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initVideoTable(List<VideoRecord> videoRecords) {
        String createVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS Video(
                    bv CHAR(%d),
                    title VARCHAR(%d),
                    owner BIGINT,
                    commit_time TIMESTAMP,
                    review_time TIMESTAMP,
                    public_time TIMESTAMP,
                    duration REAL,
                    description VARCHAR(%d),
                    reviewer BIGINT
                );
                """, MAX_BV_LENGTH, MAX_TITLE_LENGTH, MAX_DESCRIPTION_LENGTH);
        jdbcTemplate.execute(createVideoTable);
        String copySql = "COPY Video(bv, title, owner, commit_time, review_time, public_time, duration, description, reviewer) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x07', FREEZE)";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 10000;
        for (VideoRecord video : videoRecords) {
            avCount = Math.max(avCount, getAv(video.getBv()));
            if (escaper.escape(video.getTitle()).length() > MAX_TITLE_LENGTH) {
                log.info("Video bv: {}", video.getBv());
                log.info("Title's length: {}", escaper.escape(video.getTitle()).length());
                log.error("Title is too long: {}", video.getTitle());
                throw new IllegalArgumentException("Title is too long");
            }
            if (escaper.escape(video.getDescription()).length() > MAX_DESCRIPTION_LENGTH) {
                log.info("Video bv: {}", video.getBv());
                log.info("Description's length: {}", escaper.escape(video.getDescription()).length());
                log.error("Description is too long: {}", video.getDescription());
                throw new IllegalArgumentException("Description is too long");
            }
            joiner.appendTo(copyData,
                    video.getBv(),
                    escaper.escape(video.getTitle()),
                    video.getOwnerMid(),
                    video.getCommitTime(),
                    video.getReviewTime(),
                    video.getPublicTime(),
                    video.getDuration(),
                    escaper.escape(video.getDescription()),
                    video.getReviewer() == 0 ? "" : video.getReviewer()
            );
            copyData.append('\n');
            count++;
            if (count >= batchSize) {
                copyInsertion(CharSource.wrap(copyData), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createVideoTableConstraint = """
                ALTER TABLE Video ALTER COLUMN title SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN owner SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN commit_time SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN duration SET NOT NULL;
                                
                ALTER TABLE Video ADD PRIMARY KEY (bv);
                ALTER TABLE Video ADD FOREIGN KEY (owner) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE Video ADD FOREIGN KEY (reviewer) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                                
                CREATE INDEX VideoOwnerIndex ON Video(owner);
                CREATE INDEX VideoPublicTimeIndex ON Video(public_time);
                """;
        jdbcTemplate.execute(createVideoTableConstraint);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initCountVideoTable(List<VideoRecord> videoRecords, List<DanmuRecord> danmuRecords) {
        String createCountVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS CountVideo(
                    bv CHAR(%d),
                    like_count INTEGER DEFAULT 0,
                    coin_count INTEGER DEFAULT 0,
                    fav_count INTEGER DEFAULT 0,
                    view_count INTEGER DEFAULT 0,
                    view_rate DOUBLE PRECISION DEFAULT 0,
                    danmu_count INTEGER DEFAULT 0,
                    score DOUBLE PRECISION DEFAULT 0
                );
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createCountVideoTable);
        String copySql = "COPY CountVideo(bv, like_count, coin_count, fav_count, view_count, view_rate, danmu_count, score) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', FREEZE)";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 10000;
        Map<String, Long> danmuCounts = danmuRecords.stream()
                .collect(Collectors.groupingBy(DanmuRecord::getBv, Collectors.counting()));
        for (VideoRecord video : videoRecords) {
            int likeCount = video.getLike().length;
            int coinCount = video.getCoin().length;
            int favCount = video.getFavorite().length;
            int viewCount = video.getViewerMids().length;
            double totalViewTime = Floats.asList(video.getViewTime()).stream()
                    .mapToDouble(Float::doubleValue).sum();
            double viewRate = totalViewTime / video.getDuration();
            int danmuCount = danmuCounts.getOrDefault(video.getBv(), 0L).intValue();
            double score = 0;
            if (viewCount != 0) {
                score += Math.min(1, likeCount / (double) viewCount);
                score += Math.min(1, coinCount / (double) viewCount);
                score += Math.min(1, favCount / (double) viewCount);
                score += danmuCount / (double) viewCount;
                score += viewRate / (double) viewCount;
            }
            joiner.appendTo(copyData,
                    video.getBv(),
                    likeCount,
                    coinCount,
                    favCount,
                    viewCount,
                    viewRate,
                    danmuCount,
                    score
            );
            copyData.append('\n');
            count++;
            if (count >= batchSize) {
                copyInsertion(CharSource.wrap(copyData), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createCountVideoTableConstraint = """
                ALTER TABLE CountVideo ALTER COLUMN like_count SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN coin_count SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN fav_count SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN view_count SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN view_rate SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN danmu_count SET NOT NULL;
                ALTER TABLE CountVideo ALTER COLUMN score SET NOT NULL;
                                
                ALTER TABLE CountVideo ADD PRIMARY KEY (bv);
                ALTER TABLE CountVideo ADD FOREIGN KEY (bv) REFERENCES Video(bv) ON DELETE CASCADE;
                                
                CREATE INDEX CountVideoScoreIndex ON CountVideo(score DESC);
                """;
        jdbcTemplate.execute(createCountVideoTableConstraint);
        String setTrigger = """
                CREATE OR REPLACE FUNCTION calc_score()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.view_count = 0 THEN
                        NEW.score := 0;
                        RETURN NEW;
                    END IF;
                    NEW.score := LEAST(1, NEW.like_count / NEW.view_count) +
                                 LEAST(1, NEW.coin_count / NEW.view_count) +
                                 LEAST(1, NEW.fav_count / NEW.view_count) +
                                 NEW.danmu_count / NEW.view_count +
                                 NEW.view_rate / NEW.view_count;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS update_score ON CountVideo;
                CREATE TRIGGER update_score
                BEFORE INSERT OR UPDATE ON CountVideo
                FOR EACH ROW
                EXECUTE PROCEDURE calc_score();
                """;
        jdbcTemplate.execute(setTrigger);
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional(propagation = Propagation.MANDATORY)
    public void initLikeVideoTable(List<VideoRecord> VideoRecords) {
        String createLikeVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS LikeVideo(
                    mid BIGINT,
                    bv CHAR(%d)
                ) PARTITION BY HASH (mid);
                CREATE TABLE LikeVideo_1 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE LikeVideo_2 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE LikeVideo_3 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE LikeVideo_4 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createLikeVideoTable);
        String copySql = "COPY LikeVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getLike()) {
                joiner.appendTo(copyData,
                        mid,
                        bv
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        setVideoConstraint("Like");
        setTriggers("like", "LikeVideo");
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional(propagation = Propagation.MANDATORY)
    public void initCoinVideoTable(List<VideoRecord> VideoRecords) {
        String createCoinVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS CoinVideo(
                    mid BIGINT,
                    bv CHAR(%d)
                ) PARTITION BY HASH (mid);
                CREATE TABLE CoinVideo_1 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE CoinVideo_2 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE CoinVideo_3 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE CoinVideo_4 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createCoinVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY CoinVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getCoin()) {
                joiner.appendTo(copyData,
                        mid,
                        bv
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        setVideoConstraint("Coin");
        setTriggers("coin", "CoinVideo");
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional(propagation = Propagation.MANDATORY)
    public void initFavVideoTable(List<VideoRecord> videoRecords) {
        String createFavVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS FavVideo(
                    mid BIGINT,
                    bv CHAR(%d)
                ) PARTITION BY HASH (mid);
                CREATE TABLE FavVideo_1 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE FavVideo_2 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE FavVideo_3 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE FavVideo_4 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createFavVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY FavVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000;
        for (VideoRecord video : videoRecords) {
            String bv = video.getBv();
            for (long mid : video.getFavorite()) {
                joiner.appendTo(copyData,
                        mid,
                        bv
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        setVideoConstraint("Fav");
        setTriggers("fav", "FavVideo");
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initViewVideoTable(List<VideoRecord> videoRecords) {
        String createViewVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS ViewVideo(
                    mid BIGINT,
                    bv CHAR(%d),
                    view_time REAL
                ) PARTITION BY HASH (mid);
                CREATE TABLE ViewVideo_1 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE ViewVideo_2 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE ViewVideo_3 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE ViewVideo_4 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createViewVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY ViewVideo(mid, bv, view_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000;
        for (VideoRecord video : videoRecords) {
            long[] viewerMids = video.getViewerMids();
            String bv = video.getBv();
            float[] viewTimes = video.getViewTime();

            int length = video.getViewerMids().length;
            for (int i = 0; i < length; i++) {
                joiner.appendTo(copyData,
                        viewerMids[i],
                        bv,
                        viewTimes[i]
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createViewVideoTableConstraint = """
                ALTER TABLE ViewVideo ALTER COLUMN view_time SET NOT NULL;
                                
                ALTER TABLE ViewVideo ADD PRIMARY KEY (mid, bv);
                ALTER TABLE ViewVideo ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE ViewVideo ADD FOREIGN KEY (bv) REFERENCES Video(bv) ON DELETE CASCADE;
                                
                CREATE INDEX ViewVideoBvIndex ON ViewVideo(bv);
                """;
        jdbcTemplate.execute(createViewVideoTableConstraint);
        String setTriggers = """
                CREATE OR REPLACE FUNCTION increase_view_count()
                RETURNS TRIGGER AS $$
                BEGIN
                    UPDATE CountVideo
                    SET view_count = view_count + 1,
                        view_rate = view_rate + NEW.view_time / current_setting('sustc.temp_duration')::REAL
                    WHERE bv = NEW.bv;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS update_view_count ON ViewVideo;
                CREATE TRIGGER update_view_count
                AFTER INSERT ON ViewVideo
                FOR EACH ROW
                EXECUTE PROCEDURE increase_view_count();
                                
                CREATE OR REPLACE FUNCTION decrease_view_count()
                RETURNS TRIGGER AS $$
                BEGIN
                    UPDATE CountVideo
                    SET view_count = view_count - 1,
                        view_rate = view_rate - OLD.view_time / current_setting('sustc.temp_duration')::REAL
                    WHERE bv = OLD.bv;
                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS delete_view_count ON ViewVideo;
                CREATE TRIGGER delete_view_count
                AFTER DELETE ON ViewVideo
                FOR EACH ROW
                EXECUTE PROCEDURE decrease_view_count();
                """;
        jdbcTemplate.execute(setTriggers);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void initDanmuTable(List<DanmuRecord> danmuRecords) {
        String createDanmuTable = String.format("""
                CREATE TABLE IF NOT EXISTS Danmu(
                    id BIGSERIAL,
                    bv CHAR(%d),
                    mid BIGINT,
                    dis_time REAL,
                    content VARCHAR(%d),
                    post_time TIMESTAMP
                );
                """, MAX_BV_LENGTH, MAX_CONTENT_LENGTH);
        jdbcTemplate.execute(createDanmuTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY Danmu(id, bv, mid, dis_time, content, post_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\x07', FREEZE)";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 10000, danmuId = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuId++;
            if (escaper.escape(danmu.getContent()).length() > MAX_CONTENT_LENGTH) {
                log.info("Danmu id: {}", danmuId);
                log.info("Content's length: {}", escaper.escape(danmu.getContent()).length());
                log.error("Content is too long: {}", danmu.getContent());
                throw new IllegalArgumentException("Content is too long.");
            }
            joiner.appendTo(copyData,
                    danmuId,
                    danmu.getBv(),
                    danmu.getMid(),
                    danmu.getTime(),
                    escaper.escape(danmu.getContent()),
                    danmu.getPostTime()
            );
            copyData.append('\n');
            count++;
            if (count >= batchSize) {
                copyInsertion(CharSource.wrap(copyData), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createDanmuTableConstraint = """
                SELECT setval(pg_get_serial_sequence('Danmu', 'id'), (SELECT MAX(id) FROM Danmu));
                                
                ALTER TABLE Danmu ALTER COLUMN bv SET NOT NULL;
                ALTER TABLE Danmu ALTER COLUMN mid SET NOT NULL;
                ALTER TABLE Danmu ALTER COLUMN dis_time SET NOT NULL;
                ALTER TABLE Danmu ALTER COLUMN content SET NOT NULL;
                ALTER TABLE Danmu ALTER COLUMN post_time SET NOT NULL;
                                
                ALTER TABLE Danmu ADD PRIMARY KEY (id);
                ALTER TABLE Danmu ADD FOREIGN KEY (bv) REFERENCES Video(bv) ON DELETE CASCADE;
                ALTER TABLE Danmu ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                                
                CREATE INDEX DanmuBvDisTimeIndex ON Danmu(bv, dis_time);
                CREATE INDEX DanmuContentPostTimeIndex ON Danmu(content, post_time);
                """;
        jdbcTemplate.execute(createDanmuTableConstraint);
        setTriggers("danmu", "Danmu");
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional(propagation = Propagation.MANDATORY)
    public void initLikeDanmuTable(List<DanmuRecord> danmuRecords) {
        String createLikeDanmuTable = """
                CREATE TABLE IF NOT EXISTS LikeDanmu(
                    mid BIGINT,
                    id BIGINT
                ) PARTITION BY HASH (id);
                CREATE TABLE LikeDanmu_1 PARTITION OF LikeDanmu FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE LikeDanmu_2 PARTITION OF LikeDanmu FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE LikeDanmu_3 PARTITION OF LikeDanmu FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE LikeDanmu_4 PARTITION OF LikeDanmu FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createLikeDanmuTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY LikeDanmu(mid, id) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 50000, danmuID = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuID++;
            for (long mid : danmu.getLikedBy()) {
                joiner.appendTo(copyData,
                        mid,
                        danmuID
                );
                copyData.append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(CharSource.wrap(copyData), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(CharSource.wrap(copyData), copySql);
        }
        String createLikeDanmuTableConstraint = """
                ALTER TABLE LikeDanmu ADD PRIMARY KEY (mid, id);
                ALTER TABLE LikeDanmu ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE LikeDanmu ADD FOREIGN KEY (id) REFERENCES Danmu(id) ON DELETE CASCADE;
                                
                CREATE INDEX LikeDanmuIdIndex ON LikeDanmu(id);
                """;
        jdbcTemplate.execute(createLikeDanmuTableConstraint);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void copyInsertion(CharSource copyData, String copySql) {
        jdbcTemplate.execute((ConnectionCallback<Long>) connection -> {
            var copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            try {
                return copyManager.copyIn(copySql, copyData.openStream());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void setTriggers(String type, String table) {
        String setTriggers = """
                CREATE OR REPLACE FUNCTION increase_${TYPE}_count()
                RETURNS TRIGGER AS $$
                BEGIN
                    UPDATE CountVideo
                    SET ${TYPE}_count = ${TYPE}_count + 1
                    WHERE bv = NEW.bv;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS update_${TYPE}_count ON ${TABLE};
                CREATE TRIGGER update_${TYPE}_count
                AFTER INSERT ON ${TABLE}
                FOR EACH ROW
                EXECUTE PROCEDURE increase_${TYPE}_count();
                                
                CREATE OR REPLACE FUNCTION decrease_${TYPE}_count()
                RETURNS TRIGGER AS $$
                BEGIN
                    UPDATE CountVideo
                    SET ${TYPE}_count = ${TYPE}_count - 1
                    WHERE bv = OLD.bv;
                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS delete_${TYPE}_count ON ${TABLE};
                CREATE TRIGGER delete_${TYPE}_count
                AFTER DELETE ON ${TABLE}
                FOR EACH ROW
                EXECUTE PROCEDURE decrease_${TYPE}_count();
                """
                .replace("${TYPE}", type)
                .replace("${TABLE}", table);
        //noinspection SqlSourceToSinkFlow
        jdbcTemplate.execute(setTriggers);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void setVideoConstraint(String table) {
        String setVideoConstrain = """
                ALTER TABLE ${TABLE}Video ADD PRIMARY KEY (mid, bv);
                ALTER TABLE ${TABLE}Video ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE;
                ALTER TABLE ${TABLE}Video ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE;
                                
                CREATE INDEX ${TABLE}VideoBvIndex ON ${TABLE}Video(bv);
                """
                .replace("${TABLE}", table);
        //noinspection SqlSourceToSinkFlow
        jdbcTemplate.execute(setVideoConstrain);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
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

        initUserAuthTable(userRecords);

        initUserProfileTable(userRecords);

        initUserFollowTable(userRecords);

        log.info("Importing User table finished.");

        initVideoTable(videoRecords);

        initCountVideoTable(videoRecords, danmuRecords);

        initLikeVideoTable(videoRecords);

        initCoinVideoTable(videoRecords);

        initFavVideoTable(videoRecords);

        initViewVideoTable(videoRecords);

        log.info("Importing Video table finished.");

        initDanmuTable(danmuRecords);

        createGetHotspotFunction();

        initLikeDanmuTable(danmuRecords);

        log.info("Importing Danmu table finished.");

        jdbcTemplate.execute("ANALYZE;");

        log.info("End importing at " + new Timestamp(new Date().getTime()));
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
            return jdbcTemplate.queryForObject(sql, Long.class, name) != null;
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
        Pattern pattern = Pattern.compile("(\\d{1,2})月(\\d{1,2})日");
        Matcher matcher = pattern.matcher(req.getBirthday());
        sql = "INSERT INTO UserProfile(mid, name, sex, birthday_month, birthday_day, level, coin, sign, identity) VALUES (?, ?, ?, ?, 1, 0, ?, ?::Identity)";
        jdbcTemplate.update(sql, mid, req.getName(), req.getSex(), Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)), req.getSign(), UserRecord.Identity.USER.name());
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
        String sql = "SELECT bv FROM Video WHERE owner = ? AND title = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, mid, title) != null;
        } catch (EmptyResultDataAccessException e) {
            return false;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public String insertVideo(long mid, PostVideoReq req) {
        String sql = "INSERT INTO Video(bv, title, owner, commit_time, duration, description) VALUES (?, ?, ?, LOCALTIMESTAMP, ?, ?)";
        String bv = generateBV();
        jdbcTemplate.update(sql, bv, req.getTitle(), mid, req.getDuration(), req.getDescription());
        return bv;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean deleteVideo(String bv) {
        String sql = "DELETE FROM Video WHERE bv = ?";
        return jdbcTemplate.update(sql, bv) > 0;
    }

    @Override
    public PostVideoReq getVideoReq(String bv) {
        String sql = "SELECT title, duration, description, public_time FROM Video WHERE bv = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> PostVideoReq.builder()
                    .title(rs.getString("title").replace("\\n", "\n").replace("\\t", "\t"))
                    .duration(rs.getFloat("duration"))
                    .description(rs.getString("description").replace("\\n", "\n").replace("\\t", "\t"))
                    .publicTime(rs.getTimestamp("public_time"))
                    .build(), bv);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean updateVideoInfo(String bv, PostVideoReq req) {
        String sql = "UPDATE Video SET title = ?, duration = ?, description = ?, public_time = ?, reviewer = NULL, review_time = NULL WHERE bv = ?";
        return jdbcTemplate.update(sql, req.getTitle(), req.getDuration(), req.getDescription(), req.getPublicTime(), bv) > 0;
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
                ORDER BY relevance DESC, view_count DESC
                WHERE relevance > 0
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
