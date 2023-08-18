/*
 * Copyright (c) 2013-2023 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.runway.validation.EmailValidator;

/**
 * Repro GH-139
 *
 * @author Jeff Nelson
 */
public class GH_139ReproTest extends RunwayBaseClientServerTest {
    
    @Test
    public void reproNoValidationCheckForNoValues() {
        Player player = new Player();
        if(!player.save()) {
            player.throwSupressedExceptions();
            Assert.fail();
        }
        Assert.assertTrue(true);
    }
    
    @Test
    public void reproValidationCheckForEmptyValue() {
        Player player = new Player();
        player.emails.add("");
        if(player.save()) {
            Assert.fail();
        }
        else {
            try {
                player.throwSupressedExceptions();
            }
            catch(Exception e) {
                Assert.assertTrue(true);
            }
        }
    }
    
    class Player extends Record {
        
        @ValidatedBy(EmailValidator.class)
        public Set<String> emails = new HashSet<>();
    }

}
