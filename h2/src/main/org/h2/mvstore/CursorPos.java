/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A position in a cursor.
 * Instance represents a node in the linked list, which traces path
 * fom a specific (target) key within a leaf node all the way up to te root
 * (bottom up path).
 */
public class CursorPos implements RootReference.VisitablePages
{
    /**
     * The page at the current level.
     */
    public Page page;

    /**
     * Index of the key (within page above) used to go down to a lower level
     * in case of intermediate nodes, or index of the target key for leaf a node.
     * In a later case, it could be negative, if the key is not present.
     */
    public int index;

    /**
     * Next node in the linked list, representing the position within parent level,
     * or null, if we are at the root level already.
     */
    public CursorPos parent;

    public CursorPos(Page page, int index, CursorPos parent) {
        this.page = page;
        this.index = index;
        this.parent = parent;
    }

    @Override
    public void visitPages(Page.Visitor visitor) {
        for (CursorPos head = this; head != null; head = head.parent) {
            Page page = head.page;
            if (page.getTotalCount() > 0) {
                long pagePos = page.getPos();
                visitor.visit(page, pagePos);
            }
        }
    }

    void markRemovedPages() {
        ConcurrentHashMap<Long, Long> toBeDeleted = page.map.getStore().pagesToBeDeleted;
        for (CursorPos head = this; head != null; head = head.parent) {
            Page page = head.page;
            if (page.getTotalCount() > 0) {
                long pagePos = page.getPos();
                toBeDeleted.put(page.id, pagePos);
                if (DataUtils.isPageSaved(pagePos)) {
                    toBeDeleted.put(pagePos, page.id);
                }
            }
        }
    }

    long[] collectRemovedPagePositions(MVMap.IntValueHolder unsavedMemoryHolder, long version) {
        int count = 0;
        int unsavedMemory = 0;
        for (CursorPos head = this; head != null; head = head.parent) {
            Page page = head.page;
            if (page.isSaved()) {
                if (DataUtils.getPageChunkId(page.getPos()) <= version) {
                    ++count;
                }
            } else {
                unsavedMemory += page.getMemory();
            }
        }
        unsavedMemoryHolder.value -= unsavedMemory;
        if (count == 0) {
            return null;
        }
        long[] positions = new long[count];
        count = 0;
        for (CursorPos head = this; head != null; head = head.parent) {
            long pagePos = head.page.getPos();
            if (DataUtils.isPageSaved(pagePos) && DataUtils.getPageChunkId(pagePos) <= version) {
                positions[count++] = pagePos;
            }
        }
        return positions;
    }
}

