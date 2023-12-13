package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DatabaseService;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {
    private final DatabaseService databaseService;

    @Autowired
    public RecommenderServiceImpl(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Override
    public List<String> recommendNextVideo(String bv) {
        if (bv == null || bv.isEmpty())
            return null;
        float duration = databaseService.getValidVideoDuration(bv);
        if (duration < 0)
            return null;
        return databaseService.getTopVideos(bv);
    }

    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if (pageSize <= 0 || pageNum <= 0)
            return null;
        return databaseService.getRecVideos(pageSize, pageNum);
    }

    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        return null;
    }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        return null;
    }
}
