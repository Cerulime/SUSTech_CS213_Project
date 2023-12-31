package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.UserRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    private final DatabaseService databaseService;
    private final UserService userService;
    private List<String> lastKeywords;

    @Autowired
    public VideoServiceImpl(DatabaseService databaseService, UserService userService) {
        this.databaseService = databaseService;
        this.userService = userService;
        lastKeywords = null;
    }

    @Override
    @Transactional
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        if (userService.invalidAuthInfo(auth))
            return null;
        if (req == null || req.isInvalid()) {
            log.warn("Invalid video info: {}", req);
            return null;
        }
        if (databaseService.isSameVideoExist(auth.getMid(), req.getTitle())) {
            log.warn("Same video title exist: {}", req);
            return null;
        }
        return databaseService.insertVideo(auth.getMid(), req);
    }

    @Override
    @Transactional
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        long owner = databaseService.getVideoOwner(bv);
        if (owner < 0) {
            log.warn("Video not found: {}", bv);
            return false;
        }
        UserRecord.Identity identity = databaseService.getUserIdentity(auth.getMid());
        if (identity == UserRecord.Identity.SUPERUSER || owner == auth.getMid())
            return databaseService.deleteVideo(bv);
        log.warn("User {} is not allowed to delete video {}", auth.getMid(), bv);
        return false;
    }

    @Override
    @Transactional
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        if (userService.invalidAuthInfo(auth) || req == null || req.isInvalid())
            return false;
        long owner = databaseService.getVideoOwner(bv);
        if (owner < 0 || owner != auth.getMid()) {
            log.warn("User {} is not allowed to update video {}", auth.getMid(), bv);
            return false;
        }
        if (!databaseService.isNewInfoValid(bv, req)) {
            log.warn("Invalid new video info: {}", req);
            return false;
        }
        boolean isReviewed = databaseService.isVideoReviewed(bv);
        boolean res = databaseService.updateVideoInfo(bv, req);
        if (!res) {
            log.error("Failed to update video info: {}", req);
            throw new RuntimeException("Failed to update video info");
        }
        return isReviewed;
    }

    @Override
    @Transactional
    public synchronized List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (userService.invalidAuthInfo(auth))
            return null;
        if (pageSize <= 0 || pageNum <= 0) {
            log.warn("Invalid page size or number: {} {}", pageSize, pageNum);
            return null;
        }
        List<String> keyword = Arrays.stream(keywords.replace("\t", "").split(" "))
                .filter(s -> !s.isEmpty()).sorted().collect(Collectors.toList());
        if (keyword.equals(lastKeywords)) {
            databaseService.createTempTable(auth.getMid());
            for (String s : keyword)
                databaseService.updateRelevanceTemp(s.toLowerCase());
            databaseService.mergeTemp(auth.getMid());
        } else {
            lastKeywords = keyword;
            databaseService.resetUnloggedTable(auth.getMid());
            for (String s : keyword)
                databaseService.updateRelevance(s.toLowerCase());
        }
        return databaseService.searchVideo(pageSize, pageNum);
    }

    @Override
    public double getAverageViewRate(String bv) {
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0) {
            log.warn("Video not found: {}", bv);
            return -1;
        }
        return databaseService.getAverageViewRate(bv);
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        if (bv == null || bv.isEmpty() || !databaseService.isDanmuExistByBv(bv)) {
            log.warn("getHotspot corner case: {}", bv);
            return new HashSet<>();
        }
        return databaseService.getHotspot(bv);
    }

    @Override
    @Transactional
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth) ||
                databaseService.getUserIdentity(auth.getMid()) != UserRecord.Identity.SUPERUSER) {
            log.warn("User {} is not allowed to review video {}", auth.getMid(), bv);
            return false;
        }
        long owner = databaseService.getVideoOwner(bv);
        if (owner < 0 || owner == auth.getMid()) {
            log.warn("User {} is not allowed to review video {}", auth.getMid(), bv);
            return false;
        }
        if (databaseService.isVideoReviewed(bv)) {
            log.warn("Video {} has been reviewed", bv);
            return false;
        }
        return databaseService.reviewVideo(auth.getMid(), bv);
    }

    @Override
    @Transactional
    public boolean coinVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        if (bv == null || databaseService.isVideoNotEngage(auth, bv)) {
            log.warn("Invalid video for engaging: {}", bv);
            return false;
        }
        int coin = databaseService.getCoin(auth.getMid());
        if (coin < 1) {
            log.warn("User {} has no coin", auth.getMid());
            return false;
        }
        boolean isCoin = databaseService.coinVideo(auth.getMid(), bv);
        if (isCoin)
            databaseService.updateCoin(auth.getMid(), coin - 1);
        return isCoin;
    }

    @Override
    @Transactional
    public boolean likeVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        if (bv == null || databaseService.isVideoNotEngage(auth, bv)) {
            log.warn("Invalid video for engaging: {}", bv);
            return false;
        }
        boolean isLike = databaseService.isVideoLiked(auth.getMid(), bv);
        if (isLike)
            return !databaseService.unlikeVideo(auth.getMid(), bv);
        else
            return databaseService.likeVideo(auth.getMid(), bv);
    }

    @Override
    @Transactional
    public boolean collectVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        if (bv == null || databaseService.isVideoNotEngage(auth, bv)) {
            log.warn("Invalid video for engaging: {}", bv);
            return false;
        }
        boolean isCollect = databaseService.isVideoCollected(auth.getMid(), bv);
        if (isCollect)
            return !databaseService.uncollectVideo(auth.getMid(), bv);
        else
            return databaseService.collectVideo(auth.getMid(), bv);
    }
}
