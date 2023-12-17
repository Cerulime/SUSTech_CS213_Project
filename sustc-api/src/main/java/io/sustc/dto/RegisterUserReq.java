package io.sustc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

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

    private boolean isValidBirthday(String birthday) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu年M月d日")
                .withResolverStyle(java.time.format.ResolverStyle.STRICT);
        try {
            //noinspection ResultOfMethodCallIgnored
            LocalDate.parse("2000年" + birthday, formatter);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

    // QQ and WeChat are optional, special check in UserServiceImpl
    public boolean isValid() {
        return password != null && !password.isEmpty() &&
                name != null && !name.isEmpty() &&
                sex != null &&
                (birthday == null || !birthday.isEmpty() || isValidBirthday(birthday));
    }
}
