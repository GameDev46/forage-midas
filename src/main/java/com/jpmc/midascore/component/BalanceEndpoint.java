package com.jpmc.midascore.component;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.entity.UserRecord;

@RestController
public class BalanceEndpoint {
    
    private final DatabaseConduit databaseConduit;

    public BalanceEndpoint(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam(name = "userId") long userId) {

        // Find the requested user
        UserRecord user = databaseConduit.findUserById(userId);
        // If the user doesn't exist then return a balance of 0
        if (user == null) return new Balance(0f);

        // Return the user's balance
        return new Balance(user.getBalance());
    }

}
