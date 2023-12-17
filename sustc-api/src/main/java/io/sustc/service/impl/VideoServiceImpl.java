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
        if (req == null || req.isInvalid())
            return null;
        if (databaseService.isSameVideoExist(auth.getMid(), req.getTitle()))
            return null;
        return databaseService.insertVideo(auth.getMid(), req);
    }

    @Override
    @Transactional
    public boolean deleteVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        if (bv == null || bv.isEmpty())
            return false;
        long owner = databaseService.getVideoOwner(bv);
        if (owner < 0)
            return false;
        UserRecord.Identity identity = databaseService.getUserIdentity(auth.getMid());
        if (identity == UserRecord.Identity.SUPERUSER || owner == auth.getMid())
            return databaseService.deleteVideo(bv);
        return false;
    }

    @Override
    @Transactional
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        if (userService.invalidAuthInfo(auth) || req == null || req.isInvalid())
            return false;
        long owner = databaseService.getVideoOwner(bv);
        if (owner < 0 || owner != auth.getMid())
            return false;
        PostVideoReq origin = databaseService.getVideoReq(bv);
        if (origin.equals(req) || origin.getDuration() != req.getDuration())
            return false;
        return databaseService.updateVideoInfo(bv, req);
    }

    @Override
    @Transactional
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        if (userService.invalidAuthInfo(auth) || pageSize <= 0 || pageNum <= 0)
            return null;
        if (lastKeywords == null)
            databaseService.createUnloggedTable(auth.getMid());
        List<String> keyword = Arrays.stream(keywords.split(" "))
                .filter(s -> !s.isEmpty()).sorted().collect(Collectors.toList());
        if (keyword.equals(lastKeywords)) {
            databaseService.updateUnloggedTable(auth.getMid());
            return databaseService.searchVideo(pageSize, pageNum);
        }
        lastKeywords = keyword;
        databaseService.resetUnloggedTable(auth.getMid());
        for (String s : keyword)
            databaseService.updateRelevance(s.toLowerCase());
        return databaseService.searchVideo(pageSize, pageNum);
    }

    @Override
    public double getAverageViewRate(String bv) {
        if (bv == null || bv.isEmpty())
            return -1;
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0)
            return -1;
        double viewTime = databaseService.getVideoViewTime(bv);
        if (viewTime < 0)
            return -1;
        return viewTime / duration;
    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        if (bv == null || bv.isEmpty() || !databaseService.isDanmuExistByBv(bv))
            return new HashSet<>();
        return databaseService.getHotspot(bv);
    }

    @Override
    @Transactional
    public boolean reviewVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth) ||
                databaseService.getUserIdentity(auth.getMid()) != UserRecord.Identity.SUPERUSER)
            return false;
        long owner = databaseService.getVideoOwner(bv);
        if (bv == null || owner < 0 || owner == auth.getMid())
            return false;
        if (databaseService.isVideoReviewed(bv))
            return false;
        return databaseService.reviewVideo(auth.getMid(), bv);
    }

    @Override
    @Transactional
    public boolean coinVideo(AuthInfo auth, String bv) {
        if (userService.invalidAuthInfo(auth))
            return false;
        if (bv == null || databaseService.isVideoNotEngage(auth, bv))
            return false;
        int coin = databaseService.getCoin(auth.getMid());
        if (coin < 1)
            return false;
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
        if (bv == null || databaseService.isVideoNotEngage(auth, bv))
            return false;
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
        if (bv == null || databaseService.isVideoNotEngage(auth, bv))
            return false;
        boolean isCollect = databaseService.isVideoCollected(auth.getMid(), bv);
        if (isCollect)
            return !databaseService.uncollectVideo(auth.getMid(), bv);
        else
            return databaseService.collectVideo(auth.getMid(), bv);
    }
}
