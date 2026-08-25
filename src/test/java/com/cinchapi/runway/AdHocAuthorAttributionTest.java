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
package com.cinchapi.runway;

import org.junit.Assert;
import org.junit.Test;

import com.cinchapi.common.reflect.Reflection;
import com.cinchapi.runway.access.Audience;

/**
 * Unit tests for the author attribution that a save stores when the
 * {@link Audience} is an {@link AdHocRecord}.
 *
 * @author Jeff Nelson
 */
public class AdHocAuthorAttributionTest extends RunwayBaseClientServerTest {

    /**
     * <strong>Goal:</strong> Verify that a save stores no author attribution
     * when the author is one the database can never hold.
     * <p>
     * <strong>Start state:</strong> An {@link Agent}, which is never stored,
     * and an unsaved {@link Document}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Write to the {@link Document} on behalf of the {@link Agent}.</li>
     * <li>Save the {@link Document}.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The database stores no author for the
     * {@link Document}.
     */
    @Test
    public void testSaveStoresNoAuthorWhenAudienceIsAdHoc() {
        Agent agent = new Agent();
        agent.name = "relay";
        Document document = new Document();
        agent.write("text", "Written by an ephemeral audience", document);
        Assert.assertTrue(document.save());
        Assert.assertTrue(
                client.select(Record.AUTHOR_KEY, document.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a save stores no author attribution
     * when the author is one the database can never hold, even when the
     * {@link Document} already carries an attribution that a save stored.
     * <p>
     * <strong>Start state:</strong> A saved {@link Document} attributed to a
     * saved {@link Editor}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Write to the {@link Document} on behalf of a saved {@link Editor},
     * then save both.</li>
     * <li>Write to the {@link Document} on behalf of an {@link Agent}, then
     * save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The database stores an author after the first
     * save and none after the second.
     */
    @Test
    public void testSaveClearsStoredAuthorWhenAudienceIsAdHoc() {
        Editor editor = new Editor();
        editor.name = "uma";
        Document document = new Document();
        editor.write("text", "Written by an editor", document);
        Assert.assertTrue(runway.save(editor, document));
        Assert.assertFalse(
                client.select(Record.AUTHOR_KEY, document.id()).isEmpty());
        Agent agent = new Agent();
        agent.name = "relay";
        agent.write("text", "Rewritten by an ephemeral audience", document);
        Assert.assertTrue(document.save());
        Assert.assertTrue(
                client.select(Record.AUTHOR_KEY, document.id()).isEmpty());
    }

    /**
     * <strong>Goal:</strong> Verify that a save whose only write would be an
     * attribution that a save cannot store does not refuse a {@link Document}
     * whose data another writer erased, because such a save writes nothing.
     * <p>
     * <strong>Start state:</strong> A saved {@link Document} that a second
     * instance deletes, attributed to an {@link Agent}.
     * <p>
     * <strong>Workflow:</strong>
     * <ul>
     * <li>Save a {@link Document}.</li>
     * <li>Load a second instance of it and delete that instance.</li>
     * <li>Attribute the first instance to an {@link Agent}, changing no field,
     * then save it.</li>
     * </ul>
     * <p>
     * <strong>Expected:</strong> The save succeeds and the {@link Document}
     * still holds no data.
     */
    @Test
    public void testAdHocAuthorOnlySaveSucceedsWhenRecordWasDeleted() {
        Document subject = new Document();
        subject.text = "original";
        Assert.assertTrue(subject.save());
        Document other = runway.load(Document.class, subject.id());
        other.deleteOnSave();
        Assert.assertTrue(runway.save(other));
        Agent agent = new Agent();
        agent.name = "relay";
        Reflection.set("_author", agent, subject); // (authorized)
        Assert.assertTrue(runway.save(subject));
        Assert.assertTrue(client.select(subject.id()).isEmpty());
    }

    /**
     * An {@link Audience} that the database never holds, which stands in for
     * any caller that acts through an {@link AdHocRecord}.
     */
    static class Agent extends AdHocRecord implements Audience {

        /**
         * The name of the agent.
         */
        String name;
    }

    /**
     * An {@link Audience} that a save stores.
     */
    static class Editor extends Record implements Audience {

        /**
         * The name of the editor.
         */
        String name;
    }

    /**
     * A {@link Record} that an {@link Audience} writes to.
     */
    static class Document extends Record {

        /**
         * The document's body.
         */
        String text;
    }

}
