package io.sustc.service;

import io.sustc.dto.*;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface DatabaseService {

    /**
     * Acknowledges the authors of this project.
     *
     * @return a list of group members' student-id
     */
    List<Integer> getGroupMembers();

    /**
     * Imports data to an empty database.
     * Invalid data will not be provided.
     *
     * @param danmuRecords danmu records parsed from csv
     * @param userRecords  user records parsed from csv
     * @param videoRecords video records parsed from csv
     */
    void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    );

    /**
     * Truncates all tables in the database.
     * <p>
     * This would only be used in local benchmarking to help you
     * clean the database without dropping it, and won't affect your score.
     */
    void truncate();

    /**
     * Sums up two numbers via Postgres.
     * This method only demonstrates how to access database via JDBC.
     *
     * @param a the first number
     * @param b the second number
     * @return the sum of two numbers
     */
    Integer sum(int a, int b);

    int MAX_PASSWORD_LENGTH = 96;
    int MAX_QQ_LENGTH = 10;
    int MAX_WECHAT_LENGTH = 25;

    int MAX_NAME_LENGTH = 20;
    int MAX_SIGN_LENGTH = 50;

    int MAX_BV_LENGTH = 12;
    int MAX_TITLE_LENGTH = 80;
    int MAX_DESCRIPTION_LENGTH = 2000;

    int MAX_CONTENT_LENGTH = 100;

    AuthInfo getAuthInfo(long mid);

    AuthInfo getAuthInfoByQq(String qq);

    AuthInfo getAuthInfoByWechat(String wechat);

    boolean isMidNotExist(long mid);

    boolean isQQorWechatExist(String qq, String wechat);

    boolean isNameExist(String name);

    long insertUser(RegisterUserReq req);

    UserRecord.Identity getUserIdentity(long mid);

    boolean deleteUser(long mid);

    boolean isFollowing(long followerMid, long followeeMid);

    boolean follow(long followerMid, long followeeMid);

    boolean unfollow(long followerMid, long followeeMid);

    int getCoin(long mid);

    @Async
    CompletableFuture<long[]> getFollowingAsync(long mid);

    long[] getFollowing(long mid);

    @Async
    CompletableFuture<long[]> getFollowerAsync(long mid);

    long[] getFollower(long mid);

    @Async
    CompletableFuture<String[]> getWatchedAsync(long mid);

    String[] getWatched(long mid);

    @Async
    CompletableFuture<String[]> getLikedAsync(long mid);

    String[] getLiked(long mid);

    @Async
    CompletableFuture<String[]> getCollectedAsync(long mid);

    String[] getCollected(long mid);

    @Async
    CompletableFuture<String[]> getPostedAsync(long mid);

    String[] getPosted(long mid);
}
