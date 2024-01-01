package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.sustc.service.DatabaseService.*;

@Service
@EnableAsync
@Slf4j
public class AsyncInitTable {
    private final JdbcTemplate jdbcTemplate;
    private final Transformer transformer;

    @Autowired
    public AsyncInitTable(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transformer = new Transformer();
    }

    private String escape(String input) {
        if (input == null)
            return "";
        return input.replace("\t", "\\t")
                .replace("\n", "\\n");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initUserAuthTableAsync(List<UserRecord> userRecords) {
        return CompletableFuture.runAsync(() -> initUserAuthTable(userRecords));
    }

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
        AtomicBoolean hasError = new AtomicBoolean(false);
        String copyData = userRecords.parallelStream().map(user -> {
            String qq = user.getQq();
            if (qq != null && qq.length() > MAX_QQ_LENGTH) {
                log.error("QQ is too long: {}", user.getQq());
                hasError.set(true);
                return null;
            }
            String wechat = user.getWechat();
            if (wechat != null && wechat.length() > MAX_WECHAT_LENGTH) {
                log.error("WeChat is too long: {}", user.getWechat());
                hasError.set(true);
                return null;
            }
            String encodedPassword = UserService.encodePassword(user.getPassword());
            if (qq == null) qq = "";
            if (wechat == null) wechat = "";
            return String.join("\t", String.valueOf(user.getMid()), encodedPassword, qq, wechat);
        }).filter(s -> s != null && !hasError.get()).collect(Collectors.joining("\n"));
        if (hasError.get()) {
            throw new IllegalArgumentException("One or more records have errors. Check logs for details.");
        }
        log.info("Finish encoding passwords");
        String copySql = "COPY UserAuth(mid, password, qq, wechat) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        copyInsertion(copyData, copySql);
        String createUserAuthTableConstraint = """
                SELECT setval(pg_get_serial_sequence('UserAuth', 'mid'), (SELECT MAX(mid) FROM UserAuth));
                                
                ALTER TABLE UserAuth ALTER COLUMN password SET NOT NULL;
                                
                ALTER TABLE UserAuth ADD PRIMARY KEY (mid);
                ALTER TABLE UserAuth ADD CONSTRAINT UniqueQq UNIQUE (qq);
                ALTER TABLE UserAuth ADD CONSTRAINT UniqueWechat UNIQUE (wechat);
                                
                CREATE INDEX UserAuthQqIndex ON UserAuth (qq);
                CREATE INDEX UserAuthWechatIndex ON UserAuth (wechat);
                """;
        jdbcTemplate.execute(createUserAuthTableConstraint);
        log.info("Finish initializing UserAuth table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initUserProfileTableAsync(List<UserRecord> userRecords) {
        return CompletableFuture.runAsync(() -> initUserProfileTable(userRecords));
    }

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
            String escapeName = escape(user.getName());
            if (escapeName.length() > MAX_NAME_LENGTH) {
                log.info("User mid: {}", user.getMid());
                log.error("Name is too long: {}", user.getName());
                throw new IllegalArgumentException("Name is too long");
            }
            String escapeSign = escape(user.getSign());
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
            copyData.append(user.getMid()).append('\t')
                    .append(escapeName).append('\t')
                    .append(user.getSex() == null ? "" : user.getSex()).append('\t')
                    .append(isEmpty ? "" : matcher.group(1)).append('\t')
                    .append(isEmpty ? "" : matcher.group(2)).append('\t')
                    .append(user.getLevel()).append('\t')
                    .append(user.getCoin()).append('\t')
                    .append(escapeSign).append('\t')
                    .append(user.getIdentity().name()).append('\n');
        }
        String copySql = "COPY UserProfile(mid, name, sex, birthday_month, birthday_day, level, coin, sign, identity) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x07')";
        copyInsertion(copyData.toString(), copySql);
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
        log.info("Finish initializing UserProfile table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initUserFollowTableAsync(List<UserRecord> userRecords) {
        return CompletableFuture.runAsync(() -> initUserFollowTable(userRecords));
    }

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
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (UserRecord user : userRecords) {
            for (long followee : user.getFollowing()) {
                copyData.append(user.getMid()).append('\t')
                        .append(followee).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
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
        log.info("Finish initializing UserFollow table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initVideoTableAsync(List<VideoRecord> videoRecords) {
        return CompletableFuture.runAsync(() -> initVideoTable(videoRecords));
    }

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
        String copySql = "COPY Video(bv, title, owner, commit_time, review_time, public_time, duration, description, reviewer) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE E'\\x07')";
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (VideoRecord video : videoRecords) {
            transformer.setAvCount(Math.max(transformer.getAvCount(), transformer.getAv(video.getBv())));
            String escapeTitle = escape(video.getTitle());
            if (escapeTitle.length() > MAX_TITLE_LENGTH) {
                log.info("Video bv: {}", video.getBv());
                log.info("Title's length: {}", escapeTitle.length());
                log.error("Title is too long: {}", video.getTitle());
                throw new IllegalArgumentException("Title is too long");
            }
            String escapeDescription = escape(video.getDescription());
            if (escapeDescription.length() > MAX_DESCRIPTION_LENGTH) {
                log.info("Video bv: {}", video.getBv());
                log.info("Description's length: {}", escapeDescription.length());
                log.error("Description is too long: {}", video.getDescription());
                throw new IllegalArgumentException("Description is too long");
            }
            copyData.append(video.getBv()).append('\t')
                    .append(escapeTitle).append('\t')
                    .append(video.getOwnerMid()).append('\t')
                    .append(video.getCommitTime()).append('\t')
                    .append(video.getReviewTime()).append('\t')
                    .append(video.getPublicTime()).append('\t')
                    .append(video.getDuration()).append('\t')
                    .append(escapeDescription).append('\t')
                    .append(video.getReviewer()).append('\n');
            count++;
            if (count >= NORMAL_BATCH_SIZE) {
                copyInsertion(copyData.toString(), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
        }
        String createVideoTableConstraint = """
                ALTER TABLE Video ALTER COLUMN title SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN owner SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN commit_time SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN duration SET NOT NULL;
                ALTER TABLE Video ALTER COLUMN public_time SET NOT NULL;
                                
                ALTER TABLE Video ADD PRIMARY KEY (bv);
                ALTER TABLE Video ADD FOREIGN KEY (owner) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE Video ADD FOREIGN KEY (reviewer) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                                
                CREATE INDEX VideoOwnerIndex ON Video(owner);
                CREATE INDEX VideoPublicTimeIndex ON Video(public_time);
                """;
        jdbcTemplate.execute(createVideoTableConstraint);
        log.info("Finish initializing Video table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initCountVideoTableAsync(List<VideoRecord> videoRecords, List<DanmuRecord> danmuRecords) {
        return CompletableFuture.runAsync(() -> initCountVideoTable(videoRecords, danmuRecords));
    }

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
        String copySql = "COPY CountVideo(bv, like_count, coin_count, fav_count, view_count, view_rate, danmu_count, score) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        Map<String, Long> danmuCounts = danmuRecords.stream()
                .collect(Collectors.groupingBy(DanmuRecord::getBv, Collectors.counting()));
        for (VideoRecord video : videoRecords) {
            int likeCount = video.getLike().length;
            int coinCount = video.getCoin().length;
            int favCount = video.getFavorite().length;
            int viewCount = video.getViewerMids().length;
            float[] viewTimes = video.getViewTime();
            double totalViewTime = 0;
            for (float viewTime : viewTimes) {
                totalViewTime += viewTime;
            }
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
            copyData.append(video.getBv()).append('\t')
                    .append(likeCount).append('\t')
                    .append(coinCount).append('\t')
                    .append(favCount).append('\t')
                    .append(viewCount).append('\t')
                    .append(viewRate).append('\t')
                    .append(danmuCount).append('\t')
                    .append(score).append('\n');
            count++;
            if (count >= NORMAL_BATCH_SIZE) {
                copyInsertion(copyData.toString(), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
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
        log.info("Finish initializing CountVideo table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initLikeVideoTableAsync(List<VideoRecord> VideoRecords) {
        return CompletableFuture.runAsync(() -> initLikeVideoTable(VideoRecords));
    }

    @SuppressWarnings("DuplicatedCode")
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
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getLike()) {
                copyData.append(mid).append('\t')
                        .append(bv).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
        }
        setVideoConstraint("Like");
        setTriggers("like", "LikeVideo");
        log.info("Finish initializing LikeVideo table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initCoinVideoTableAsync(List<VideoRecord> VideoRecords) {
        return CompletableFuture.runAsync(() -> initCoinVideoTable(VideoRecords));
    }

    @SuppressWarnings("DuplicatedCode")
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
        String copySql = "COPY CoinVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getCoin()) {
                copyData.append(mid).append('\t')
                        .append(bv).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
        }
        setVideoConstraint("Coin");
        setTriggers("coin", "CoinVideo");
        log.info("Finish initializing CoinVideo table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initFavVideoTableAsync(List<VideoRecord> videoRecords) {
        return CompletableFuture.runAsync(() -> initFavVideoTable(videoRecords));
    }

    @SuppressWarnings("DuplicatedCode")
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
        String copySql = "COPY FavVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (VideoRecord video : videoRecords) {
            String bv = video.getBv();
            for (long mid : video.getFavorite()) {
                copyData.append(mid).append('\t')
                        .append(bv).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
        }
        setVideoConstraint("Fav");
        setTriggers("fav", "FavVideo");
        log.info("Finish initializing FavVideo table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initViewVideoTableAsync(List<VideoRecord> videoRecords) {
        return CompletableFuture.runAsync(() -> initViewVideoTable(videoRecords));
    }

    public void initViewVideoTable(List<VideoRecord> videoRecords) {
        String createViewVideoTable = String.format("""
                CREATE TABLE IF NOT EXISTS ViewVideo(
                    mid BIGINT,
                    bv CHAR(%d),
                    view_time REAL
                ) PARTITION BY HASH (mid);
                CREATE TABLE ViewVideo_1 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 0);
                CREATE TABLE ViewVideo_2 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 1);
                CREATE TABLE ViewVideo_3 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 2);
                CREATE TABLE ViewVideo_4 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 3);
                CREATE TABLE ViewVideo_5 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 4);
                CREATE TABLE ViewVideo_6 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 5);
                CREATE TABLE ViewVideo_7 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 6);
                CREATE TABLE ViewVideo_8 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 8, REMAINDER 7);
                """, MAX_BV_LENGTH);
        jdbcTemplate.execute(createViewVideoTable);
        String copySql = "COPY ViewVideo(mid, bv, view_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0;
        for (VideoRecord video : videoRecords) {
            long[] viewerMids = video.getViewerMids();
            String bv = video.getBv();
            float[] viewTimes = video.getViewTime();
            int length = video.getViewerMids().length;
            for (int i = 0; i < length; i++) {
                copyData.append(viewerMids[i]).append('\t')
                        .append(bv).append('\t')
                        .append(viewTimes[i]).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
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
                DECLARE
                    video_duration REAL;
                BEGIN
                    SELECT duration INTO video_duration FROM Video WHERE bv = NEW.bv;
                    UPDATE CountVideo
                    SET view_count = view_count + 1,
                        view_rate = view_rate + NEW.view_time / video_duration
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
                DECLARE
                    video_duration REAL;
                BEGIN
                    SELECT duration INTO video_duration FROM Video WHERE bv = OLD.bv;
                    UPDATE CountVideo
                    SET view_count = view_count - 1,
                        view_rate = view_rate - OLD.view_time / video_duration
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
        log.info("Finish initializing ViewVideo table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initDanmuTableAsync(List<DanmuRecord> danmuRecords) {
        return CompletableFuture.runAsync(() -> initDanmuTable(danmuRecords));
    }

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
        String copySql = "COPY Danmu(id, bv, mid, dis_time, content, post_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\x07')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, danmuId = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuId++;
            if (escape(danmu.getContent()).length() > MAX_CONTENT_LENGTH) {
                log.info("Danmu id: {}", danmuId);
                log.info("Content's length: {}", escape(danmu.getContent()).length());
                log.error("Content is too long: {}", danmu.getContent());
                throw new IllegalArgumentException("Content is too long.");
            }
            copyData.append(danmuId).append('\t')
                    .append(danmu.getBv()).append('\t')
                    .append(danmu.getMid()).append('\t')
                    .append(danmu.getTime()).append('\t')
                    .append(escape(danmu.getContent())).append('\t')
                    .append(danmu.getPostTime()).append('\n');
            count++;
            if (count >= NORMAL_BATCH_SIZE) {
                copyInsertion(copyData.toString(), copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
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
        log.info("Finish initializing Danmu table");
    }

    @Async("taskExecutor")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public CompletableFuture<Void> initLikeDanmuTableAsync(List<DanmuRecord> danmuRecords) {
        return CompletableFuture.runAsync(() -> initLikeDanmuTable(danmuRecords));
    }

    @SuppressWarnings("DuplicatedCode")
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
        String copySql = "COPY LikeDanmu(mid, id) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, danmuID = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuID++;
            for (long mid : danmu.getLikedBy()) {
                copyData.append(mid).append('\t')
                        .append(danmuID).append('\n');
                count++;
                if (count >= BIG_BATCH_SIZE) {
                    copyInsertion(copyData.toString(), copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData.toString(), copySql);
        }
        String createLikeDanmuTableConstraint = """
                ALTER TABLE LikeDanmu ADD PRIMARY KEY (mid, id);
                ALTER TABLE LikeDanmu ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid) ON DELETE CASCADE;
                ALTER TABLE LikeDanmu ADD FOREIGN KEY (id) REFERENCES Danmu(id) ON DELETE CASCADE;
                                
                CREATE INDEX LikeDanmuIdIndex ON LikeDanmu(id);
                """;
        jdbcTemplate.execute(createLikeDanmuTableConstraint);
        log.info("Finish initializing LikeDanmu table");
    }

    //    @Transactional(propagation = Propagation.MANDATORY)
    public void copyInsertion(String copyData, String copySql) {
        jdbcTemplate.execute((ConnectionCallback<Long>) connection -> {
            var copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            try (Reader reader = new StringReader(copyData)) {
                return copyManager.copyIn(copySql, reader);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    //    @Transactional(propagation = Propagation.MANDATORY)
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

    //    @Transactional(propagation = Propagation.MANDATORY)
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

    public long getAvCount() {
        return transformer.getAvCount();
    }
}
