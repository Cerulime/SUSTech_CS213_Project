package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DatabaseService;
import io.sustc.service.RecommenderService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    private final DatabaseService databaseService;
    private final UserService userService;

    @Autowired
    public RecommenderServiceImpl(DatabaseService databaseService, UserService userService) {
        this.databaseService = databaseService;
        this.userService = userService;
    }

    @Override
    public List<String> recommendNextVideo(String bv) {
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0) {
            log.warn("Video {} not found", bv);
            return null;
        }
        return databaseService.getTopVideos(bv);
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageSize <= 0 || pageNum <= 0) {
            log.warn("Invalid pageSize {} or pageNum {}", pageSize, pageNum);
            return null;
        }
        return databaseService.getRecVideos(pageSize, pageNum);
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        if (userService.invalidAuthInfo(auth))
            return null;
        if (pageSize <= 0 || pageNum <= 0) {
            log.warn("Invalid pageSize {} or pageNum {}", pageSize, pageNum);
            return null;
        }
        if (databaseService.isInterestsExist(auth.getMid()))
            return databaseService.getRecVideosForUser(auth.getMid(), pageSize, pageNum);
        else
            return databaseService.getRecVideos(pageSize, pageNum);
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        if (userService.invalidAuthInfo(auth))
            return null;
        if (pageSize <= 0 || pageNum <= 0) {
            log.warn("Invalid pageSize {} or pageNum {}", pageSize, pageNum);
            return null;
        }
        return databaseService.getRecFriends(auth.getMid(), pageSize, pageNum);
    }
}
