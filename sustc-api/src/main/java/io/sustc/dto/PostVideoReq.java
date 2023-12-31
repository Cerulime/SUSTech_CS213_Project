package io.sustc.dto;

import io.sustc.service.DatabaseService;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PostVideoReq implements Serializable {

    /**
     * The video's title.
     */
    private String title;

    /**
     * The video's description.
     */
    private String description;

    /**
     * The video's duration (in seconds).
     */
    private float duration;

    /**
     * The video's public time.
     * <p>
     * When posting a video, the owner can decide when to make it public.
     * Before the public time, the video is only visible to the owner and superusers.
     * <p>
     * This field can't be null.
     */
    private Timestamp publicTime;

    public boolean isInvalid() {
        return title == null || title.isEmpty() ||
                duration < 10 ||
                publicTime == null || publicTime.before(Timestamp.valueOf(LocalDateTime.now()));
    }

    public boolean isSame(PostVideoReq req) {
        return title.equals(req.title) &&
                description.equals(req.description) &&
                Math.abs(duration - req.duration) < DatabaseService.EPSILON &&
                publicTime.equals(req.publicTime);

    }
}
