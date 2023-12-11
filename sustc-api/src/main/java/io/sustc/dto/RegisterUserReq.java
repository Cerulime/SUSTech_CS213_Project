package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RegisterUserReq implements Serializable {

    private String password;

    private String qq;

    private String wechat;

    private String name;

    private Gender sex;

    private String birthday;

    private String sign;

    public enum Gender {
        MALE,
        FEMALE,
        UNKNOWN,
    }
}
