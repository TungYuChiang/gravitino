/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestTagEvent {
  private TagEventDispatcher dispatcher;
  private TagEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Tag tag;

  @BeforeAll
  void init() {
    this.tag = mockTag();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    TagDispatcher tagDispatcher = mockTagDispatcher();
    this.dispatcher = new TagEventDispatcher(eventBus, tagDispatcher);
    TagDispatcher tagExceptionDispatcher = mockExceptionTagDispatcher();
    this.failureDispatcher = new TagEventDispatcher(eventBus, tagExceptionDispatcher);
  }

  @Test
  void testCreateTagEvent() {
    dispatcher.createTag("metalake", tag.name(), tag.comment(), tag.properties());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateTagEvent.class, event.getClass());
    TagInfo tagInfo = ((CreateTagEvent) event).createdTagInfo();
    checkTagInfo(tagInfo, tag);
    Assertions.assertEquals(OperationType.CREATE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testGetTagEvent() {
    String metalake = "metalake";

    dispatcher.getTag(metalake, tag.name());
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetTagEvent.class, event.getClass());
    Assertions.assertEquals(tag.name(), ((GetTagEvent) event).tagName());
    Assertions.assertEquals(OperationType.GET_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testListTagEvent() {
    dispatcher.listTags("metalake");
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((ListTagEvent) event).metalake());
    Assertions.assertEquals(OperationType.LIST_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testListTagInfoEvent() {
    Tag[] result = dispatcher.listTagsInfo("metalake");
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagInfoEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.LISTINFO_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(result, ((ListTagInfoEvent) event).tags());
  }

  @Test
  void testAlterTagEvent() {
    TagChange change1 = TagChange.rename("newName");
    TagChange change2 = TagChange.updateComment("new comment");
    TagChange change3 = TagChange.setProperty("key", "value");
    TagChange change4 = TagChange.removeProperty("oldKey");
    TagChange[] changes = new TagChange[] {change1, change2, change3, change4};
    dispatcher.alterTag("metalake", tag.name(), changes);
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterTagEvent.class, event.getClass());
    TagInfo tagInfo = ((AlterTagEvent) event).updatedTagInfo();
    checkTagInfo(tagInfo, tag);
    TagChange[] tagChanges = ((AlterTagEvent) event).tagChanges();
    Assertions.assertEquals(4, tagChanges.length);
    Assertions.assertEquals(change1, tagChanges[0]);
    Assertions.assertEquals(change2, tagChanges[1]);
    Assertions.assertEquals(change3, tagChanges[2]);
    Assertions.assertEquals(change4, tagChanges[3]);
    Assertions.assertEquals(OperationType.ALTER_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testDeleteTagEvent() {
    dispatcher.deleteTag("metalake", tag.name());
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteTagEvent.class, event.getClass());
    Assertions.assertEquals(OperationType.DELETE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testGetTagForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.getTagForMetadataObject("metalake", metadataObject, tag.name());
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(GetTagForMetadataObjectEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((GetTagForMetadataObjectEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((GetTagForMetadataObjectEvent) event).tagName());
    Assertions.assertEquals(
        metadataObject, ((GetTagForMetadataObjectEvent) event).metadataObject());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testListMetadataObjectsForTagEvent() {
    dispatcher.listMetadataObjectsForTag("metalake", tag.name());
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(ListMetadataObjectsForTagEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((ListMetadataObjectsForTagEvent) event).metalake());
    Assertions.assertEquals(tag.name(), ((ListMetadataObjectsForTagEvent) event).tagName());
    Assertions.assertEquals(OperationType.LIST_METADATA_OBJECTS_FOR_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testAssociateTagsForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    String[] tagsToAssociate = new String[] {"tag1", "tag2"};
    String[] tagsToDisassociate = new String[] {"tag3", "tag4"};

    dispatcher.associateTagsForMetadataObject(
        "metalake", metadataObject, tagsToAssociate, tagsToDisassociate);
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(AssociateTagsForMetadataObjectEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((AssociateTagsForMetadataObjectEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((AssociateTagsForMetadataObjectEvent) event).metadataObject());
    Assertions.assertArrayEquals(
        tagsToAssociate, ((AssociateTagsForMetadataObjectEvent) event).tagsToAdd());
    Assertions.assertArrayEquals(
        tagsToDisassociate, ((AssociateTagsForMetadataObjectEvent) event).tagsToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testListTagsForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.listTagsForMetadataObject("metalake", metadataObject);
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(ListTagsForMetadataObjectEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((ListTagsForMetadataObjectEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((ListTagsForMetadataObjectEvent) event).metadataObject());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testListTagsInfoForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.listTagsInfoForMetadataObject("metalake", metadataObject);
    Event event = dummyEventListener.popPostEvent();

    Assertions.assertEquals(ListTagsInfoForMetadataObjectEvent.class, event.getClass());
    Assertions.assertEquals("metalake", ((ListTagsInfoForMetadataObjectEvent) event).metalake());
    Assertions.assertEquals(
        metadataObject, ((ListTagsInfoForMetadataObjectEvent) event).metadataObject());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
  }

  @Test
  void testCreateTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.createTag("metalake", tag.name(), tag.comment(), tag.properties()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreateTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(tag.name(), ((CreateTagFailureEvent) event).tagInfo().name());
    Assertions.assertEquals(tag.comment(), ((CreateTagFailureEvent) event).tagInfo().comment());
    Assertions.assertEquals(
        tag.properties(), ((CreateTagFailureEvent) event).tagInfo().properties());
    Assertions.assertEquals(OperationType.CREATE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.getTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetTagForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getTagForMetadataObject("metalake", metadataObject, tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetTagForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetTagForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_TAG_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDeleteTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.deleteTag("metalake", tag.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeleteTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DeleteTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterTagFailureEvent() {
    TagChange change1 = TagChange.rename("newName");
    TagChange change2 = TagChange.updateComment("new comment");
    TagChange change3 = TagChange.setProperty("key", "value");
    TagChange change4 = TagChange.removeProperty("oldKey");
    TagChange[] changes = new TagChange[] {change1, change2, change3, change4};
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterTag("metalake", tag.name(), changes));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterTagFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterTagFailureEvent) event).exception().getClass());
    Assertions.assertEquals(changes, ((AlterTagFailureEvent) event).changes());
    Assertions.assertEquals(OperationType.ALTER_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTags("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAG, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTagsInfo("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTagsInfoFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_TAGS_INFO, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListTagsInfoForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listTagsInfoForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListTagsInfoForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListTagsInfoForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        OperationType.LIST_TAGS_INFO_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAssociateTagsForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] tagsToAssociate = new String[] {"tag1", "tag2"};
    String[] tagsToDisassociate = new String[] {"tag3", "tag4"};

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.associateTagsForMetadataObject(
                "metalake", metadataObject, tagsToAssociate, tagsToDisassociate));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AssociateTagsForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AssociateTagsForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        tagsToAssociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToAdd());
    Assertions.assertEquals(
        tagsToDisassociate, ((AssociateTagsForMetadataObjectFailureEvent) event).tagsToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_TAGS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkTagInfo(TagInfo actualTagInfo, Tag expectedTag) {
    Assertions.assertEquals(expectedTag.name(), actualTagInfo.name());
    Assertions.assertEquals(expectedTag.comment(), actualTagInfo.comment());
    Assertions.assertEquals(expectedTag.properties(), actualTagInfo.properties());
  }

  private Tag mockTag() {
    Tag tag = mock(Tag.class);
    when(tag.name()).thenReturn("tag");
    when(tag.comment()).thenReturn("comment");
    when(tag.properties()).thenReturn(ImmutableMap.of("color", "#FFFFFF"));
    return tag;
  }

  private TagDispatcher mockTagDispatcher() {
    TagDispatcher dispatcher = mock(TagDispatcher.class);
    String metalake = "metalake";
    String[] tagNames = new String[] {"tag1", "tag2"};
    Tag[] tags = new Tag[] {tag, tag};

    when(dispatcher.createTag(
            any(String.class), any(String.class), any(String.class), any(Map.class)))
        .thenReturn(tag);
    when(dispatcher.listTags(metalake)).thenReturn(tagNames);
    when(dispatcher.listTagsInfo(metalake)).thenReturn(tags);
    when(dispatcher.alterTag(any(String.class), any(String.class), any(TagChange[].class)))
        .thenReturn(tag);
    when(dispatcher.getTag(any(String.class), any(String.class))).thenReturn(tag);
    when(dispatcher.deleteTag(metalake, tag.name())).thenReturn(true);
    when(dispatcher.getTagForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String.class)))
        .thenReturn(tag);
    MetadataObject catalog =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    MetadataObject[] objects = new MetadataObject[] {catalog};

    when(dispatcher.listMetadataObjectsForTag(any(String.class), any(String.class)))
        .thenReturn(objects);

    when(dispatcher.associateTagsForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String[].class), any(String[].class)))
        .thenReturn(new String[] {"tag1", "tag2"});

    when(dispatcher.listTagsForMetadataObject(any(String.class), any(MetadataObject.class)))
        .thenReturn(new String[] {"tag1", "tag2"});

    when(dispatcher.listTagsInfoForMetadataObject(any(String.class), any(MetadataObject.class)))
        .thenReturn(new Tag[] {tag, tag});

    return dispatcher;
  }

  private TagDispatcher mockExceptionTagDispatcher() {
    return mock(
        TagDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }
}
