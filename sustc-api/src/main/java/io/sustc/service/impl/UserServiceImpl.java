package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;
import org.springframework.stereotype.Service;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    private final DatabaseService databaseService;

    @Autowired
    public UserServiceImpl(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    private boolean isValidBirthday(String birthday) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu年M月d日")
                .withResolverStyle(java.time.format.ResolverStyle.STRICT);
        try {
            //noinspection ResultOfMethodCallIgnored
            LocalDate.parse("2000年" + birthday, formatter);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

    @Override
    public long register(RegisterUserReq req) {
        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null ||
                req.getBirthday() == null || req.getBirthday().isEmpty() || !isValidBirthday(req.getBirthday()))
            return -1;
        if (req.getQq() != null && req.getQq().length() > DatabaseService.MAX_QQ_LENGTH) {
            log.warn("QQ is too long: {}", req.getQq());
            return -1;
        }
        if (req.getWechat() != null && req.getWechat().length() > DatabaseService.MAX_WECHAT_LENGTH) {
            log.warn("WeChat is too long: {}", req.getWechat());
            return -1;
        }
        if ((req.getQq() != null && !req.getQq().isEmpty()) ||
                (req.getWechat() != null && !req.getWechat().isEmpty()))
            if (databaseService.isQQorWechatExist(req.getQq(), req.getWechat()))
                return -1;
        if (databaseService.isNameExist(req.getName()))
            return -1;
        return databaseService.insertUser(req);
    }

    @Override
    public boolean invalidAuthInfo(AuthInfo auth) {
        if (auth == null) return true;
        if (auth.getPassword() != null && !auth.getPassword().isEmpty()) {
            AuthInfo data = databaseService.getAuthInfo(auth.getMid());
            if (data == null) return true;
            Argon2PasswordEncoder encoder = new Argon2PasswordEncoder();
            if (!encoder.matches(auth.getPassword(), data.getPassword()))
                return true;
            data.setPassword(auth.getPassword());
            return !data.equals(auth);
        }
        AuthInfo QqData = null, WechatData = null;
        if (auth.getQq() != null && !auth.getQq().isEmpty()) {
            QqData = databaseService.getAuthInfoByQq(auth.getQq());
            if (QqData == null || QqData.getMid() != auth.getMid())
                return true;
        }
        if (auth.getWechat() != null && !auth.getWechat().isEmpty()) {
            WechatData = databaseService.getAuthInfoByWechat(auth.getWechat());
            if (WechatData == null || WechatData.getMid() != auth.getMid())
                return true;
        }
        if (QqData != null && WechatData != null)
            return !QqData.equals(WechatData);
        return false;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        if (invalidAuthInfo(auth) || databaseService.isMidNotExist(mid))
            return false;
        UserRecord.Identity authIdentity = databaseService.getUserIdentity(auth.getMid());
        UserRecord.Identity midIdentity = databaseService.getUserIdentity(mid);
        if (authIdentity == UserRecord.Identity.USER &&
                auth.getMid() != mid)
            return false;
        if (authIdentity == UserRecord.Identity.SUPERUSER &&
                midIdentity == UserRecord.Identity.SUPERUSER && auth.getMid() != mid)
            return false;
        return databaseService.deleteUser(mid);
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        if (invalidAuthInfo(auth) || databaseService.isMidNotExist(followeeMid))
            return false;
        boolean isFollowed = databaseService.isFollowing(auth.getMid(), followeeMid);
        if (isFollowed)
            return databaseService.unfollow(auth.getMid(), followeeMid);
        else
            return databaseService.follow(auth.getMid(), followeeMid);
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        if (databaseService.isMidNotExist(mid))
            return null;
        CompletableFuture<long[]> followingFuture = databaseService.getFollowingAsync(mid);
        CompletableFuture<long[]> followerFuture = databaseService.getFollowerAsync(mid);
        CompletableFuture<String[]> watchedFuture = databaseService.getWatchedAsync(mid);
        CompletableFuture<String[]> likedFuture = databaseService.getLikedAsync(mid);
        CompletableFuture<String[]> collectedFuture = databaseService.getCollectedAsync(mid);
        CompletableFuture<String[]> postedFuture = databaseService.getPostedAsync(mid);
        CompletableFuture.allOf(followingFuture, followerFuture, watchedFuture, likedFuture, collectedFuture, postedFuture).join();
        return UserInfoResp.builder()
                .mid(mid)
                .coin(databaseService.getCoin(mid))
                .following(followingFuture.join())
                .follower(followerFuture.join())
                .watched(watchedFuture.join())
                .liked(likedFuture.join())
                .collected(collectedFuture.join())
                .posted(postedFuture.join())
                .build();
    }
}