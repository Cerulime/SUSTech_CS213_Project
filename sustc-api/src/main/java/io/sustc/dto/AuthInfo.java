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
public class AuthInfo implements Serializable {

    /**
     * The user's mid.
     */
    private long mid;

    /**
     * The password used when login by mid.
     */
    private String password;

    /**
     * OIDC login by QQ, does not require a password.
     */
    private String qq;

    /**
     * OIDC login by WeChat, does not require a password.
     */
    private String wechat;

    public void replace(AuthInfo data) {
        if (data == null) return;
        if (data.getMid() > 0) this.mid = data.getMid();
        if (data.getPassword() != null && !data.getPassword().isEmpty()) this.password = data.getPassword();
        if (data.getQq() != null && !data.getQq().isEmpty()) this.qq = data.getQq();
        if (data.getWechat() != null && !data.getWechat().isEmpty()) this.wechat = data.getWechat();
    }
}
