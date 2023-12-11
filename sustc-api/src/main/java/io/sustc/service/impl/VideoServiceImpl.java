package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.service.VideoService;

import java.util.List;
import java.util.Set;

public class VideoServiceImpl implements VideoService{
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {

    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {

    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {

    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {

    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        // 创建临时视图
        // 遍历keywords更新排名
        // 分页返回
    }

    @Override
    public double getAverageViewRate(String bv) {

    }

    @Override
    public Set<Integer> getHotspot(String bv) {

    }

    @Override
    public boolean reviewVideo(AuthInfo auth, String bv) {

    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {

    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {

    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {

    }
}
