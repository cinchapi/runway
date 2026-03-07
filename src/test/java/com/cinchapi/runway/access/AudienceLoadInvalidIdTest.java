/*
 * Copyright (c) 2013-2026 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cinchapi.runway.access;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests verifying the behavior when loading {@link Record Records} with
 * invalid IDs or when access is restricted through the {@link Audience} access
 * control framework.
 *
 * @author Jeff Nelson
 */
public class AudienceLoadInvalidIdTest extends AudienceAccessControlBaseTest {

    @Test
    public void testLoadInvalidIdThroughRunwayHandler() {
        // Attempt to load a record with an ID that does not exist in the
        // database through the runway handler
        long invalidId = 999999999L;
        Candidate candidate = runway.load(Candidate.class, invalidId);
        Assert.assertNull(candidate);
    }

    @Test
    public void testLoadInvalidIdThroughAudienceHandler() {
        // Create an audience to perform the load operation
        Admin admin = new Admin();
        admin.name = "System Admin";
        admin.email = "admin@system.com";
        admin.save();

        // Attempt to load a record with an ID that does not exist in the
        // database through the audience handler
        long invalidId = 999999999L;
        Candidate candidate = admin.load(Candidate.class, invalidId);
        Assert.assertNull(candidate);
    }

    @Test
    public void testLoadValidRecordNotVisibleToAudience() {
        // Create a candidate that will be the subject of the access control
        Candidate candidate = new Candidate();
        candidate.name = "Jane Developer";
        candidate.email = "jane@email.com";
        candidate.resume = "Jane's resume content";
        candidate.skills = "Java, Python";
        candidate.save();

        // Create another candidate (the audience) who cannot discover the
        // first candidate's Application
        Candidate anotherCandidate = new Candidate();
        anotherCandidate.name = "Bob Smith";
        anotherCandidate.email = "bob@email.com";
        anotherCandidate.save();

        Employer employer = new Employer();
        employer.name = "Employer";

        Job job = new Job();
        job.employer = employer;
        job.title = "Job";

        Application application = new Application();
        application.candidate = candidate;
        application.job = job;

        Assert.assertTrue(application.save());
        long applicationId = application.id();

        Application loadedA = candidate.load(Application.class, applicationId);
        Application loadedB = anotherCandidate.load(Application.class,
                applicationId);

        Assert.assertNotNull(loadedA);
        Assert.assertNull(loadedB);
    }

}
