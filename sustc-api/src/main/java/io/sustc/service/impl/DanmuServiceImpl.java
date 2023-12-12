package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (bv == null || bv.isEmpty() || content == null || content.isEmpty())
            return -1;
        if (content.length() > DatabaseService.MAX_CONTENT_LENGTH) {
            log.warn("Content is too long: {}", content);
            return -1;
        }
        if (auth == null || userService.invalidAuthInfo(auth))
            return -1;
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0)
            return -1;
        if (time < 0 || time > duration) {
            log.warn("Invalid time: {}", time);
            return -1;
        }
        if (databaseService.isVideoUnwatched(auth.getMid(), bv))
            return -1;
        return databaseService.insertDanmu(auth.getMid(), bv, content, time);
    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if (timeStart > timeEnd || timeStart < 0 || timeEnd < 0)
            return null;
        if (bv == null || bv.isEmpty())
            return null;
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0 || timeEnd > duration)
            return null;
        if (filter)
            return databaseService.getDanmuFiltered(bv, timeStart, timeEnd);
        else
            return databaseService.getDanmu(bv, timeStart, timeEnd);
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (auth == null || userService.invalidAuthInfo(auth))
            return false;
        String bv = databaseService.getBvByDanmuId(id);
        if (bv == null || bv.isEmpty() || databaseService.isVideoUnwatched(auth.getMid(), bv))
            return false;
        boolean liked = databaseService.isDanmuLiked(auth.getMid(), id);
        if (liked)
            return !databaseService.unlikeDanmu(auth.getMid(), id);
        else
            return databaseService.likeDanmu(auth.getMid(), id);
    }
}
