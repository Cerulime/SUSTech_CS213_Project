package io.sustc.service.impl;

import com.google.common.base.Joiner;
import com.google.common.io.CharSource;
import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Service
@EnableAsync
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public DatabaseServiceImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public List<Integer> getGroupMembers() {
        return List.of(12212224);
    }

    @Transactional
    public void initUserAuthTable(List<UserRecord> userRecords) {
        String createUserAuthTable = """
                CREATE TABLE IF NOT EXISTS UserAuth (
                    mid BIGSERIAL,
                    password CHAR(${MAX_PASSWORD_LENGTH}),
                    qq VARCHAR(${MAX_QQ_LENGTH}),
                    wechat VARCHAR(${MAX_WECHAT_LENGTH})
                );
                """
                .replace("${MAX_PASSWORD_LENGTH}", String.valueOf(MAX_PASSWORD_LENGTH))
                .replace("${MAX_QQ_LENGTH}", String.valueOf(MAX_QQ_LENGTH))
                .replace("${MAX_WECHAT_LENGTH}", String.valueOf(MAX_WECHAT_LENGTH));
        jdbcTemplate.execute(createUserAuthTable);
        Joiner joiner = Joiner.on('\t').useForNull("");
        StringBuilder copyData = new StringBuilder();
        for (UserRecord user : userRecords) {
            joiner.appendTo(copyData,
                    user.getMid(),
                    UserService.encodePassword(user.getPassword()),
                    user.getQq(),
                    user.getWechat()
            );
            copyData.append('\n');
        }
        String copySql = "COPY UserAuth(mid, password, qq, wechat) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        copyInsertion(CharSource.wrap(copyData), copySql);
        String createUserAuthTableConstraint = """
                SELECT setval(pg_get_serial_sequence('UserAuth', 'mid'), (SELECT MAX(mid) FROM UserAuth));
                ALTER TABLE UserAuth
                ALTER COLUMN password SET NOT NULL;
                ALTER TABLE UserAuth(
                ADD PRIMARY KEY (mid),
                ADD CONSTRAIN UniqueQq UNIQUE (qq),
                ADD CONSTRAIN UniqueWechat UNIQUE (wechat)
                );
                CREATE INDEX UserAuthQqWechatIndex ON UserAuth USING bloom (qq, wechat);
                """;
        jdbcTemplate.execute(createUserAuthTableConstraint);
    }

    @Transactional
    public void initUserProfileTable(List<UserRecord> userRecords) {
        String createUserProfileTable = """
                CREATE TABLE IF NOT EXISTS UserProfile(
                    mid BIGINT,
                    name VARCHAR(${MAX_NAME_LENGTH}),
                    sex Gender,
                    birthday DATE,
                    level SMALLINT,
                    coin INTEGER,
                    sign VARCHAR(${MAX_SIGN_LENGTH}),
                    identity Identity
                );
                """
                .replace("${MAX_NAME_LENGTH}", String.valueOf(MAX_NAME_LENGTH))
                .replace("${MAX_SIGN_LENGTH}", String.valueOf(MAX_SIGN_LENGTH));
        jdbcTemplate.execute(createUserProfileTable);
        Joiner joiner = Joiner.on('\t').useForNull("");
        StringBuilder copyData = new StringBuilder();
        for (UserRecord user : userRecords) {
            joiner.appendTo(copyData,
                    user.getMid(),
                    user.getName(),
                    user.getSex(),
                    user.getBirthday(),
                    user.getLevel(),
                    user.getCoin(),
                    user.getSign(),
                    user.getIdentity());
            copyData.append('\n');
        }
        String copySql = "COPY UserProfile(mid, name, sex, birthday, level, coin, sign, identity) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        copyInsertion(CharSource.wrap(copyData), copySql);
        String createUserProfileTableConstraint = """
                ALTER TABLE UserProfile(
                ALTER COLUMN name SET NOT NULL,
                ALTER COLUMN sex SET NOT NULL,
                ALTER COLUMN birthday SET NOT NULL,
                ALTER COLUMN level SET NOT NULL,
                ALTER COLUMN coin SET NOT NULL,
                ALTER COLUMN identity SET NOT NULL
                );
                ALTER TABLE UserProfile(
                ADD PRIMARY KEY (mid),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD CONSTRAINT UniqueName UNIQUE (name)
                );
                CREATE INDEX UserProfileNameIndex ON UserProfile USING HASH (name);
                """;
        jdbcTemplate.execute(createUserProfileTableConstraint);
    }

    @Transactional
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
        int count = 0, batchSize = 500000;
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
                ALTER TABLE UserFollow(
                ALTER COLUMN follower SET NOT NULL,
                ALTER COLUMN followee SET NOT NULL
                );
                ALTER TABLE UserFollow(
                ADD PRIMARY KEY (follower, followee),
                ADD FOREIGN KEY (follower) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (followee) REFERENCES UserAuth(mid)
                ON DELETE CASCADE
                );
                CREATE INDEX UserFolloweeIndex ON UserFollow(followee);
                """;
        jdbcTemplate.execute(createUserFollowTableConstraint);
    }

    @Transactional
    public void initVideoTable(List<VideoRecord> videoRecords) {
        String createVideoTable = """
                CREATE TABLE IF NOT EXISTS Video(
                    bv CHAR(${MAX_BV_LENGTH}),
                    title VARCHAR(${MAX_TITLE_LENGTH}),
                    owner BIGINT,
                    commit_time TIMESTAMP,
                    review_time TIMESTAMP,
                    public_time TIMESTAMP,
                    duration FLOAT,
                    description VARCHAR(${MAX_DESCRIPTION_LENGTH}),
                    reviewer BIGINT
                );
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH))
                .replace("${MAX_TITLE_LENGTH}", String.valueOf(MAX_TITLE_LENGTH))
                .replace("${MAX_DESCRIPTION_LENGTH}", String.valueOf(MAX_DESCRIPTION_LENGTH));
        jdbcTemplate.execute(createVideoTable);
        String copySql = "COPY Video(bv, title, owner, commit_time, review_time, public_time, duration, description, reviewer) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 100000;
        for (VideoRecord video : videoRecords) {
            joiner.appendTo(copyData,
                    video.getBv(),
                    video.getTitle(),
                    video.getOwnerMid(),
                    video.getCommitTime(),
                    video.getReviewTime(),
                    video.getPublicTime(),
                    video.getDuration(),
                    video.getDescription(),
                    video.getReviewer()
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
                ALTER TABLE Video(
                ALTER COLUMN title SET NOT NULL,
                ALTER COLUMN owner SET NOT NULL,
                ALTER COLUMN commit_time SET NOT NULL,
                ALTER COLUMN duration SET NOT NULL
                );
                ALTER TABLE Video(
                ADD PRIMARY KEY (bv),
                ADD FOREIGN KEY (owner) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (reviewer) REFERENCES UserAuth(mid)
                 ON DELETE CASCADE
                );
                CREATE INDEX VideoOwnerIndex ON Video(owner);
                CREATE INDEX VideoPublicTimeIndex ON Video(public_time);
                """;
        jdbcTemplate.execute(createVideoTableConstraint);
    }

    @Transactional
    public void initPublicVideoView() {
        String createPublicVideoView = """
                CREATE OR REPLACE MATERIALIZED VIEW PublicVideo AS
                SELECT Video.bv, Video.title, Video.description, UserProfile.name AS owner_name
                FROM Video JOIN UserProfile ON Video.owner = UserProfile.mid
                WHERE COALESCE(Video.public_time, '-infinity'::TIMESTAMP) < NOW();
                """;
        jdbcTemplate.execute(createPublicVideoView);
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional
    public void initLikeVideoTable(List<VideoRecord> VideoRecords) {
        String createLikeVideoTable = """
                CREATE TABLE IF NOT EXISTS LikeVideo(
                    mid BIGINT,
                    bv CHAR(${MAX_BV_LENGTH})
                ) PARTITION BY HASH (mid);
                CREATE TABLE LikeVideo_1 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE LikeVideo_2 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE LikeVideo_3 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE LikeVideo_4 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH));
        jdbcTemplate.execute(createLikeVideoTable);
        String copySql = "COPY LikeVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        Joiner joiner = Joiner.on('\t');
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
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
        String createLikeVideoTableConstraint = """
                ALTER TABLE LikeVideo(
                ADD PRIMARY KEY (mid, bv),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE
                );
                CREATE INDEX LikeVideoBvIndex ON LikeVideo(bv);
                """;
        jdbcTemplate.execute(createLikeVideoTableConstraint);
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional
    public void initCoinVideoTable(List<VideoRecord> VideoRecords) {
        String createCoinVideoTable = """
                CREATE TABLE IF NOT EXISTS CoinVideo(
                    mid BIGINT,
                    bv CHAR(${MAX_BV_LENGTH})
                ) PARTITION BY HASH (mid);
                CREATE TABLE CoinVideo_1 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE CoinVideo_2 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE CoinVideo_3 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE CoinVideo_4 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH));
        jdbcTemplate.execute(createCoinVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY CoinVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
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
        String createCoinVideoTableConstraint = """
                ALTER TABLE CoinVideo(
                ADD PRIMARY KEY (mid, bv),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE
                );
                CREATE INDEX CoinVideoBvIndex ON CoinVideo(bv);
                """;
        jdbcTemplate.execute(createCoinVideoTableConstraint);
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional
    public void initFavVideoTable(List<VideoRecord> videoRecords) {
        String createFavVideoTable = """
                CREATE TABLE IF NOT EXISTS FavVideo(
                    mid BIGINT,
                    bv CHAR(${MAX_BV_LENGTH})
                ) PARTITION BY HASH (mid);
                CREATE TABLE FavVideo_1 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE FavVideo_2 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE FavVideo_3 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE FavVideo_4 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH));
        jdbcTemplate.execute(createFavVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY FavVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
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
        String createFavVideoTableConstraint = """
                ALTER TABLE FavVideo(
                ADD PRIMARY KEY (mid, bv),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE
                );
                CREATE INDEX FavVideoBvIndex ON FavVideo(bv);
                """;
        jdbcTemplate.execute(createFavVideoTableConstraint);
    }

    @Transactional
    public void initViewVideoTable(List<VideoRecord> videoRecords) {
        String createViewVideoTable = """
                CREATE TABLE IF NOT EXISTS ViewVideo(
                    mid BIGINT,
                    bv CHAR(${MAX_BV_LENGTH}),
                    view_time FLOAT
                ) PARTITION BY HASH (mid);
                CREATE TABLE ViewVideo_1 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE ViewVideo_2 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE ViewVideo_3 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE ViewVideo_4 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH));
        jdbcTemplate.execute(createViewVideoTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY ViewVideo(mid, bv, view_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
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
                ALTER TABLE ViewVideo
                ALTER COLUMN view_time SET NOT NULL;
                ALTER TABLE ViewVideo(
                ADD PRIMARY KEY (mid, bv),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE
                );
                CREATE INDEX ViewVideoBvIndex ON ViewVideo(bv);
                """;
        jdbcTemplate.execute(createViewVideoTableConstraint);
    }

    @Transactional
    public void initDanmuTable(List<DanmuRecord> danmuRecords) {
        String createDanmuTable = """
                CREATE TABLE IF NOT EXISTS Danmu(
                    id SERIAL,
                    bv CHAR(${MAX_BV_LENGTH}),
                    mid BIGINT,
                    dis_time FLOAT,
                    content VARCHAR(${MAX_CONTENT_LENGTH}),
                    post_time TIMESTAMP
                ) PARTITION BY HASH (id);
                CREATE TABLE Danmu_1 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE Danmu_2 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE Danmu_3 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE Danmu_4 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """
                .replace("${MAX_BV_LENGTH}", String.valueOf(MAX_BV_LENGTH))
                .replace("${MAX_CONTENT_LENGTH}", String.valueOf(MAX_CONTENT_LENGTH));
        jdbcTemplate.execute(createDanmuTable);
        Joiner joiner = Joiner.on('\t');
        String copySql = "COPY Danmu(id, bv, mid, dis_time, content, post_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 100000, danmuId = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuId++;
            joiner.appendTo(copyData,
                    danmuId,
                    danmu.getBv(),
                    danmu.getMid(),
                    danmu.getTime(),
                    danmu.getContent(),
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
                ALTER TABLE Danmu(
                ALTER COLUMN bv SET NOT NULL,
                ALTER COLUMN mid SET NOT NULL,
                ALTER COLUMN dis_time SET NOT NULL,
                ALTER COLUMN content SET NOT NULL,
                ALTER COLUMN post_time SET NOT NULL
                );
                ALTER TABLE Danmu(
                ADD PRIMARY KEY (id),
                ADD FOREIGN KEY (bv) REFERENCES Video(bv)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE
                );
                CREATE INDEX DanmuBvIndex ON Danmu(bv);
                """;
        jdbcTemplate.execute(createDanmuTableConstraint);
    }

    @SuppressWarnings("DuplicatedCode")
    @Transactional
    public void initLikeDanmuTable(List<DanmuRecord> danmuRecords) {
        String createLikeDanmuTable = """
                CREATE TABLE IF NOT EXISTS LikeDanmu(
                    mid BIGINT,
                    id INT
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
        int count = 0, batchSize = 500000, danmuID = 0;
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
                ALTER TABLE LikeDanmu(
                ADD PRIMARY KEY (mid, id),
                ADD FOREIGN KEY (mid) REFERENCES UserAuth(mid)
                ON DELETE CASCADE,
                ADD FOREIGN KEY (id) REFERENCES Danmu(id)
                ON DELETE CASCADE
                );
                CREATE INDEX LikeDanmuIdIndex ON LikeDanmu(id);
                """;
        jdbcTemplate.execute(createLikeDanmuTableConstraint);
    }

    @Transactional
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

    @Override
    @Transactional
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
                CREATE TYPE Identity AS ENUM (
                    'USER',
                    'SUPERUSER'
                );
                """;
        jdbcTemplate.execute(createIdentityEnum);
        String createGenderEnum = """
                CREATE TYPE Gender AS ENUM (
                    'MALE',
                    'FEMALE',
                    'UNKNOWN'
                );
                """;
        jdbcTemplate.execute(createGenderEnum);

        initUserAuthTable(userRecords);

        initUserProfileTable(userRecords);

        initUserFollowTable(userRecords);

        initVideoTable(videoRecords);

        initPublicVideoView();

        initLikeVideoTable(videoRecords);

        initCoinVideoTable(videoRecords);

        initFavVideoTable(videoRecords);

        initViewVideoTable(videoRecords);

        initDanmuTable(danmuRecords);

        initLikeDanmuTable(danmuRecords);

        log.info("End importing at " + new Timestamp(new Date().getTime()));
    }

    @Override
    public void truncate() {
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
    @Transactional
    public long insertUser(RegisterUserReq req) {
        String sql = "INSERT INTO UserAuth(password, qq, wechat) VALUES (?, ?, ?) RETURNING mid";
        Long mid = jdbcTemplate.queryForObject(sql, Long.class,
                UserService.encodePassword(req.getPassword()),
                req.getQq(),
                req.getWechat());
        if (mid == null)
            return -1;
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
    @Transactional
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
    @Transactional
    public boolean follow(long followerMid, long followeeMid) {
        String sql = "INSERT INTO UserFollow(follower, followee) VALUES (?, ?)";
        return jdbcTemplate.update(sql, followerMid, followeeMid) > 0;
    }

    @Override
    @Transactional
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
}
