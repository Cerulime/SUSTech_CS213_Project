package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    private final DatabaseService databaseService;

    @Autowired
    public UserServiceImpl(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Override
    @Transactional
    public long register(RegisterUserReq req) {
        if (!req.isValid()) {
            log.warn("Invalid register request: {}", req);
            return -1;
        }
        if (req.getQq() != null && req.getQq().length() > DatabaseService.MAX_QQ_LENGTH) {
            log.error("QQ is too long: {}", req.getQq());
            return -1;
        }
        if (req.getWechat() != null && req.getWechat().length() > DatabaseService.MAX_WECHAT_LENGTH) {
            log.error("WeChat is too long: {}", req.getWechat());
            return -1;
        }
        if ((req.getQq() != null && !req.getQq().isEmpty()) ||
                (req.getWechat() != null && !req.getWechat().isEmpty()))
            if (databaseService.isQQorWechatExist(req.getQq(), req.getWechat())) {
                log.warn("QQ or WeChat already exists: {} {}", req.getQq(), req.getWechat());
                return -1;
            }
        if (databaseService.isNameExist(req.getName())) {
            log.warn("Name already exists: {}", req.getName());
            return -1;
        }
        return databaseService.insertUser(req);
    }

    @Override
    public boolean invalidAuthInfo(AuthInfo auth) {
        if (auth == null) return true;
        if (auth.getPassword() != null && !auth.getPassword().isEmpty()) {
            AuthInfo data = databaseService.getAuthInfo(auth.getMid());
            if (data == null) return true;
            if (!passwordEncoder.matches(auth.getPassword(), data.getPassword()))
                return true;
            data.setPassword(auth.getPassword());
            return !data.equals(auth);
        }
        AuthInfo QqData = null, WechatData = null;
        if (auth.getQq() != null && !auth.getQq().isEmpty()) {
            QqData = databaseService.getAuthInfoByQq(auth.getQq());
            if (QqData == null || (auth.getMid() != 0 && QqData.getMid() != auth.getMid()))
                return true;
        }
        if (auth.getWechat() != null && !auth.getWechat().isEmpty()) {
            WechatData = databaseService.getAuthInfoByWechat(auth.getWechat());
            if (WechatData == null || (auth.getMid() != 0 && WechatData.getMid() != auth.getMid()))
                return true;
        }
        if (QqData != null && WechatData != null)
            return !QqData.equals(WechatData);
        if (QqData != null)
            auth.replace(QqData);
        if (WechatData != null)
            auth.replace(WechatData);
        return false;
    }

    @Override
    @Transactional
    public boolean deleteAccount(AuthInfo auth, long mid) {
        if (invalidAuthInfo(auth) || databaseService.isMidNotExist(mid)) {
            log.warn("Can not delete: {}", mid);
            return false;
        }
        UserRecord.Identity authIdentity = databaseService.getUserIdentity(auth.getMid());
        if (authIdentity == UserRecord.Identity.USER &&
                auth.getMid() != mid) {
            log.warn("Insufficient privilege: {}", auth);
            return false;
        }
        if (authIdentity == UserRecord.Identity.SUPERUSER && auth.getMid() != mid &&
                databaseService.getUserIdentity(mid) == UserRecord.Identity.SUPERUSER) {
            log.warn("Insufficient privilege: {}", auth);
            return false;
        }
        new Thread(() -> databaseService.deleteUser(mid)).start();
        return true;
    }

    @Override
    @Transactional
    public boolean follow(AuthInfo auth, long followeeMid) {
        if (invalidAuthInfo(auth) || databaseService.isMidNotExist(followeeMid)) {
            log.warn("Can not follow: {}", followeeMid);
            return false;
        }
        if (auth.getMid() == followeeMid) {
            log.warn("Can not follow yourself: {}", followeeMid);
            return false;
        }
        boolean isFollowed = databaseService.isFollowing(auth.getMid(), followeeMid);
        if (isFollowed)
            new Thread(() -> databaseService.unfollow(auth.getMid(), followeeMid)).start();
        else
            new Thread(() -> databaseService.follow(auth.getMid(), followeeMid)).start();
        return !isFollowed;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        return switch (concurrency) {
            case Sync -> getUserInfoSync(mid);
            case Async -> getUserInfoAsync(mid);
        };
    }

    private UserInfoResp getUserInfoSync(long mid) {
        if (databaseService.isMidNotExist(mid)) {
            log.warn("Can not find: {}", mid);
            return null;
        }
        long[] following = databaseService.getFollowing(mid);
        long[] follower = databaseService.getFollower(mid);
        String[] watched = databaseService.getWatched(mid);
        String[] liked = databaseService.getLiked(mid);
        String[] collected = databaseService.getCollected(mid);
        String[] posted = databaseService.getPosted(mid);
        return UserInfoResp.builder()
                .mid(mid)
                .coin(databaseService.getCoin(mid))
                .following(following)
                .follower(follower)
                .watched(watched)
                .liked(liked)
                .collected(collected)
                .posted(posted)
                .build();
    }

    private UserInfoResp getUserInfoAsync(long mid) {
        if (databaseService.isMidNotExist(mid)) {
            log.warn("Can not find: {}", mid);
            return null;
        }
        CompletableFuture<long[]> followingFuture = databaseService.getFollowingAsync(mid);
        CompletableFuture<long[]> followerFuture = databaseService.getFollowerAsync(mid);
        CompletableFuture<String[]> watchedFuture = databaseService.getWatchedAsync(mid);
        CompletableFuture.allOf(followingFuture, followerFuture, watchedFuture).join();
        return UserInfoResp.builder()
                .mid(mid)
                .coin(databaseService.getCoin(mid))
                .following(followingFuture.join())
                .follower(followerFuture.join())
                .watched(watchedFuture.join())
                .liked(databaseService.getLiked(mid))
                .collected(databaseService.getCollected(mid))
                .posted(databaseService.getPosted(mid))
                .build();
    }
}