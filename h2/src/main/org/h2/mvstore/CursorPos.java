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

    CursorPos filterUnsavedPages(MVMap.IntValueHolder unsavedMemoryHolder) {
        CursorPos head = this;
        while (head != null && !head.page.isSaved()) {
            unsavedMemoryHolder.value -= head.page.getMemory();
            head = head.parent;
        }
        if (head != null) {
            CursorPos tail = head;
            CursorPos next;
            while ((next = tail.parent) != null) {
                if (!next.page.isSaved()) {
                    tail.parent = next.parent;
                    unsavedMemoryHolder.value -= next.page.getMemory();
                } else {
                    tail = next;
                }
            }
        }
        return head;
    }

    @Override
    public void visitPages(RootReference.PageVisitor visitor) {
        CursorPos cursorPos = this;
        do {
            long pagePos = cursorPos.page.getPos();
            if (DataUtils.isPageSaved(pagePos)) {
                visitor.visit(pagePos);
            }
            cursorPos = cursorPos.parent;
        } while (cursorPos != null);
    }
}

