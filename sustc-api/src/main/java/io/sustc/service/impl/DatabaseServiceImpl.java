package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    private final JdbcTemplate jdbcTemplate;
    private final UserAuthService userAuthService;

    @Autowired
    public DatabaseServiceImpl(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.userAuthService = new UserAuthService(this);
    }

    @Override
    public List<Integer> getGroupMembers() {
        return List.of(12212224);
    }

    @Transactional
    public void initUserAuthTable(List<UserRecord> userRecords) {
        String createUserAuthTable = """
                CREATE TABLE IF NOT EXISTS UserAuth (
                    mid BIGINT,
                    password CHAR(96),
                    qq VARCHAR(10),
                    wechat VARCHAR(25)
                );
                """;
        jdbcTemplate.execute(createUserAuthTable);
        StringBuilder copyData = new StringBuilder();
        for (UserRecord user : userRecords) {
            copyData.append(user.getMid())
                    .append('\t')
                    .append(userAuthService.encodePassword(user.getPassword()))
                    .append('\t')
                    .append(user.getQq() != null ? user.getQq() : "")
                    .append('\t')
                    .append(user.getWechat() != null ? user.getWechat() : "")
                    .append('\n');
        }
        String copySql = "COPY UserAuth(mid, password, qq, wechat) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        copyInsertion(copyData, copySql);
        String createUserAuthTableConstraint = """
                ALTER TABLE UserAuth
                ALTER COLUMN password SET NOT NULL;
                ALTER TABLE UserAuth
                ADD PRIMARY KEY (mid);
                """;
        jdbcTemplate.execute(createUserAuthTableConstraint);
    }

    @Transactional
    public void initUserProfileTable(List<UserRecord> userRecords) {
        String createUserProfileTable = """
                CREATE TABLE IF NOT EXISTS UserProfile(
                    mid BIGINT,
                    name VARCHAR(20),
                    sex Gender,
                    birthday DATE,
                    level SMALLINT,
                    coin INTEGER,
                    sign VARCHAR(50),
                    identity Identity
                );
                """;
        jdbcTemplate.execute(createUserProfileTable);
        StringBuilder copyData = new StringBuilder();
        for (UserRecord user : userRecords) {
            copyData.append(user.getMid())
                    .append('\t')
                    .append(user.getName())
                    .append('\t')
                    .append(user.getSex())
                    .append('\t')
                    .append(user.getBirthday())
                    .append('\t')
                    .append(user.getLevel())
                    .append('\t')
                    .append(user.getCoin())
                    .append('\t')
                    .append(user.getSign() != null ? user.getSign() : "")
                    .append('\t')
                    .append(user.getIdentity())
                    .append('\n');
        }
        String copySql = "COPY UserProfile(mid, name, sex, birthday, level, coin, sign, identity) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        copyInsertion(copyData, copySql);
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
                ON DELETE CASCADE);
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
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
        for (UserRecord user : userRecords) {
            for (long followee : user.getFollowing()) {
                copyData.append(user.getMid())
                        .append('\t')
                        .append(followee)
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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
                    bv CHAR(12),
                    title VARCHAR(80),
                    owner BIGINT,
                    commit_time TIMESTAMP,
                    review_time TIMESTAMP,
                    public_time TIMESTAMP,
                    duration FLOAT,
                    description VARCHAR(2000),
                    reviewer BIGINT
                );
                """;
        jdbcTemplate.execute(createVideoTable);
        String copySql = "COPY Video(bv, title, owner, commit_time, review_time, public_time, duration, description, reviewer) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 100000;
        for (VideoRecord video : videoRecords) {
            copyData.append(video.getBv())
                    .append('\t')
                    .append(video.getTitle())
                    .append('\t')
                    .append(video.getOwnerMid())
                    .append('\t')
                    .append(video.getCommitTime())
                    .append('\t')
                    .append(video.getReviewTime())
                    .append('\t')
                    .append(video.getPublicTime())
                    .append('\t')
                    .append(video.getDuration())
                    .append('\t')
                    .append(video.getDescription())
                    .append('\t')
                    .append(video.getReviewer())
                    .append('\n');
            count++;
            if (count >= batchSize) {
                copyInsertion(copyData, copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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

    @Transactional
    public void initLikeVideoTable(List<VideoRecord> VideoRecords) {
        String createLikeVideoTable = """
                CREATE TABLE IF NOT EXISTS LikeVideo(
                    mid BIGINT,
                    bv CHAR(12)
                ) PARTITION BY HASH (mid);
                CREATE TABLE LikeVideo_1 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE LikeVideo_2 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE LikeVideo_3 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE LikeVideo_4 PARTITION OF LikeVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createLikeVideoTable);
        String copySql = "COPY LikeVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getLike()) {
                copyData.append(mid)
                        .append('\t')
                        .append(bv)
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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

    @Transactional
    public void initCoinVideoTable(List<VideoRecord> VideoRecords) {
        String createCoinVideoTable = """
                CREATE TABLE IF NOT EXISTS CoinVideo(
                    mid BIGINT,
                    bv CHAR(12)
                ) PARTITION BY HASH (mid);
                CREATE TABLE CoinVideo_1 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE CoinVideo_2 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE CoinVideo_3 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE CoinVideo_4 PARTITION OF CoinVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createCoinVideoTable);
        String copySql = "COPY CoinVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
        for (VideoRecord video : VideoRecords) {
            String bv = video.getBv();
            for (long mid : video.getCoin()) {
                copyData.append(mid)
                        .append('\t')
                        .append(bv)
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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

    @Transactional
    public void initFavVideoTable(List<VideoRecord> videoRecords) {
        String createFavVideoTable = """
                CREATE TABLE IF NOT EXISTS FavVideo(
                    mid BIGINT,
                    bv CHAR(12)
                ) PARTITION BY HASH (mid);
                CREATE TABLE FavVideo_1 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE FavVideo_2 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE FavVideo_3 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE FavVideo_4 PARTITION OF FavVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createFavVideoTable);
        String copySql = "COPY FavVideo(mid, bv) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
        for (VideoRecord video : videoRecords) {
            String bv = video.getBv();
            for (long mid : video.getFavorite()) {
                copyData.append(mid)
                        .append('\t')
                        .append(bv)
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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
                    bv CHAR(12),
                    view_time FLOAT
                ) PARTITION BY HASH (mid);
                CREATE TABLE ViewVideo_1 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE ViewVideo_2 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE ViewVideo_3 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE ViewVideo_4 PARTITION OF ViewVideo FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createViewVideoTable);
        String copySql = "COPY ViewVideo(mid, bv, view_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000;
        for (VideoRecord video : videoRecords) {
            long[] viewerMids = video.getViewerMids();
            String bv = video.getBv();
            float[] viewTimes = video.getViewTime();

            int length = video.getViewerMids().length;
            for (int i = 0; i < length; i++) {
                copyData.append(viewerMids[i])
                        .append('\t')
                        .append(bv)
                        .append('\t')
                        .append(viewTimes[i])
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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
                    bv CHAR(12),
                    mid BIGINT,
                    dis_time FLOAT,
                    content VARCHAR(100),
                    post_time TIMESTAMP
                ) PARTITION BY HASH (id);
                CREATE TABLE Danmu_1 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 0);
                CREATE TABLE Danmu_2 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 1);
                CREATE TABLE Danmu_3 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 2);
                CREATE TABLE Danmu_4 PARTITION OF Danmu FOR VALUES WITH (MODULUS 4, REMAINDER 3);
                """;
        jdbcTemplate.execute(createDanmuTable);
        String copySql = "COPY Danmu(id, bv, mid, dis_time, content, post_time) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 100000, danmuId = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuId++;
            copyData.append(danmuId)
                    .append('\t')
                    .append(danmu.getBv())
                    .append('\t')
                    .append(danmu.getMid())
                    .append('\t')
                    .append(danmu.getTime())
                    .append('\t')
                    .append(danmu.getContent())
                    .append('\t')
                    .append(danmu.getPostTime())
                    .append('\n');
            count++;
            if (count >= batchSize) {
                copyInsertion(copyData, copySql);
                copyData.setLength(0);
                count = 0;
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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
        String copySql = "COPY LikeDanmu(mid, id) FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')";
        StringBuilder copyData = new StringBuilder();
        int count = 0, batchSize = 500000, danmuID = 0;
        for (DanmuRecord danmu : danmuRecords) {
            danmuID++;
            for (long mid : danmu.getLikedBy()) {
                copyData.append(mid)
                        .append('\t')
                        .append(danmuID)
                        .append('\n');
                count++;
                if (count >= batchSize) {
                    copyInsertion(copyData, copySql);
                    copyData.setLength(0);
                    count = 0;
                }
            }
        }
        if (count > 0) {
            copyInsertion(copyData, copySql);
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
    public void copyInsertion(StringBuilder copyData, String copySql) {
        jdbcTemplate.execute((ConnectionCallback<Long>) connection -> {
            var copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            try {
                return copyManager.copyIn(copySql, new StringReader(copyData.toString()));
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
}
