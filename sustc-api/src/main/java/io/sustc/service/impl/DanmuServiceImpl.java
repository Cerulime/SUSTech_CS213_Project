package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    private final DatabaseService databaseService;
    private final UserService userService;

    @Autowired
    public DanmuServiceImpl(DatabaseService databaseService, UserService userService) {
        this.databaseService = databaseService;
        this.userService = userService;
    }

    @Override
    @Transactional
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (content == null || content.isEmpty()) {
            log.warn("Content is null or empty: {}", content);
            return -1;
        }
        if (content.length() > DatabaseService.MAX_CONTENT_LENGTH) {
            log.error("Content is too long: {}", content);
            return -1;
        }
        if (userService.invalidAuthInfo(auth))
            return -1;
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0) {
            log.warn("Invalid bv: {}", bv);
            return -1;
        }
        if (time < 0 || time > duration) {
            log.warn("Invalid time: {}", time);
            return -1;
        }
        if (databaseService.isVideoUnwatched(auth.getMid(), bv)) {
            log.warn("User {} has not watched video {}", auth.getMid(), bv);
            return -1;
        }
        return databaseService.insertDanmu(auth.getMid(), bv, content, time);
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (timeStart > timeEnd || timeStart < 0 || timeEnd < 0) {
            log.warn("Invalid time range: {} - {}", timeStart, timeEnd);
            return null;
        }
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0) {
            log.warn("Invalid bv: {}", bv);
            return null;
        }
        if (timeEnd > duration) {
            log.warn("Time end is too large: {}", timeEnd);
            return null;
        }
        if (filter)
            return databaseService.getDanmuFiltered(bv, timeStart, timeEnd);
        else
            return databaseService.getDanmu(bv, timeStart, timeEnd);
    }

    @Override
    @Transactional
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (userService.invalidAuthInfo(auth))
            return false;
        String bv = databaseService.getBvByDanmuId(id);
        if (bv == null || bv.isEmpty()) {
            log.warn("Invalid danmu id: {}", id);
            return false;
        }
        if (databaseService.isVideoUnwatched(auth.getMid(), bv)) {
            log.warn("User {} has not watched video {}", auth.getMid(), bv);
            return false;
        }
        boolean liked = databaseService.isDanmuLiked(auth.getMid(), id);
        if (liked)
            return !databaseService.unlikeDanmu(auth.getMid(), id);
        else
            return databaseService.likeDanmu(auth.getMid(), id);
    }
}
