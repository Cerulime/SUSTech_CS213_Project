package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import org.springframework.security.crypto.argon2.Argon2PasswordEncoder;

@Service
@Slf4j
public class UserAuthService {
    @Bean
    private Argon2PasswordEncoder passwordEncoder() { // char 96 for postgresql
        int saltLength = 16;
        int hashLength = 32;
        int parallelism = 2;
        int memory = 8192;
        int iterations = 3;
        return new Argon2PasswordEncoder(saltLength, hashLength, parallelism, memory, iterations);
    }

    private final Argon2PasswordEncoder passwordEncoder;
    private final DatabaseService databaseService;

    @Autowired
    public UserAuthService(DatabaseService databaseService) {
        this.passwordEncoder = passwordEncoder();
        this.databaseService = databaseService;
    }

    public String encodePassword(String password) {
        return passwordEncoder.encode(password);
    }

    public AuthInfo getHashedPassword(AuthInfo authInfo) {
        return AuthInfo.builder()
                .mid(authInfo.getMid())
                .password(encodePassword(authInfo.getPassword()))
                .qq(authInfo.getQq())
                .wechat(authInfo.getWechat())
                .build();
    }

    public boolean checkPassword(AuthInfo authInfo) {
        AuthInfo dataBase = databaseService.getAuthInfo(authInfo.getMid());
        if (dataBase == null)
            return false;
        return passwordEncoder().matches(authInfo.getPassword(), dataBase.getPassword());
    }
}
