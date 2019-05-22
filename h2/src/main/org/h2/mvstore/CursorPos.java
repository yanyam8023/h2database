/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

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
            if (page.getTotalCount() > 0 || !page.isLeaf()) {
                long pagePos = DataUtils.createRemovedPagePos(page.getPos(), page.map.isSingleWriter());
                visitor.visit(page, pagePos);
            }
        }
    }

    RootReference.VisitablePages shrinkRemovalInfo(MVMap.IntValueHolder unsavedMemoryHolder, long version) {
        int count = 0;
        int unsavedMemory = 0;
        for (CursorPos head = this; head != null; head = head.parent) {
            Page page = head.page;
            long pagePos = page.getPos();
            if (DataUtils.isPageSaved(pagePos) && DataUtils.getPageChunkId(pagePos) <= version) {
                ++count;
            } else if (page.getTotalCount() > 0) {
                if (page.markAsRemoved()) {
                    unsavedMemory += page.getMemory();
                } else if (DataUtils.getPageChunkId(page.getPos()) <= version) {
                    assert DataUtils.isPageSaved(page.getPos());
                    ++count;
                }
            }
        }
        unsavedMemoryHolder.value -= unsavedMemory;
        if (count == 0) {
            return null;
        }
        final long[] positions = new long[count];
        for (CursorPos head = this; head != null; head = head.parent) {
            Page page = head.page;
            long pagePos = page.getPos();
            if (DataUtils.isPageSaved(pagePos) && DataUtils.getPageChunkId(pagePos) <= version) {
                if (--count < 0) {
                    assert !page.map.store.isNoStoreInProgressFor(version);
                    return this;
                }
                positions[count] = DataUtils.createRemovedPagePos(pagePos, page.map.isSingleWriter());
            }
        }
        return new RootReference.RemovalInfo(positions);
    }
}

