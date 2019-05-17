/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

/**
 * Class RootReference is an immutable structure to represent state of the MVMap as a whole
 * (not related to a particular B-Tree node).
 * Single structure would allow for non-blocking atomic state change.
 * The most important part of it is a reference to the root node.
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RootReference
{
    /**
     * The root page.
     */
    public final Page root;
    /**
     * The version used for writing.
     */
    public final long version;
    /**
     * Indicator that map is locked for update.
     */
    final boolean lockedForUpdate;
    /**
     * Reference to the previous root in the chain.
     * That is the last root of the previous version, which had any data changes.
     * Versions without any data changes are dropped from the chain, as it built.
     */
    volatile RootReference previous;
    /**
     * Counter for successful root updates.
     */
    final long updateCounter;
    /**
     * Counter for attempted root updates.
     */
    final long updateAttemptCounter;
    /**
     * Size of the occupied part of the append buffer.
     */
    private final byte appendCounter;

    /**
     * Head of the linked list of RootReference.VisitablePages
     */
    private volatile RemovalInfoNode removalInfo;


    // This one is used to set root initially and for r/o snapshots
    RootReference(Page root, long version) {
        this.root = root;
        this.version = version;
        this.previous = null;
        this.updateCounter = 1;
        this.updateAttemptCounter = 1;
        this.lockedForUpdate = false;
        this.appendCounter = 0;
        this.removalInfo = null;
    }

    private RootReference(RootReference r, Page root, long updateAttemptCounter, VisitablePages removedPositions) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + updateAttemptCounter;
        this.lockedForUpdate = false;
        this.appendCounter = r.appendCounter;
        this.removalInfo = removedPositions == null ? r.removalInfo :
                                                    new RemovalInfoNode(removedPositions, r.removalInfo);
    }

    // This one is used for locking
    private RootReference(RootReference r, int attempt) {
        this.root = r.root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.lockedForUpdate = true;
        this.appendCounter = r.appendCounter;
        this.removalInfo = r.removalInfo;
    }

    // This one is used for unlocking
    private RootReference(RootReference r, Page root, int appendCounter, boolean lockedForUpdate,
                          VisitablePages removedPositions) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter;
        this.updateAttemptCounter = r.updateAttemptCounter;
        this.lockedForUpdate = lockedForUpdate;
        this.appendCounter = (byte) appendCounter;
        this.removalInfo = removedPositions == null ? r.removalInfo :
                                                    new RemovalInfoNode(removedPositions, r.removalInfo);
    }

    // This one is used for version change
    private RootReference(RootReference r, RootReference previous, long version, int attempt) {
        this.root = r.root;
        this.version = version;
        this.previous = previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        assert !previous.lockedForUpdate;
        this.lockedForUpdate = false;
//        this.lockedForUpdate = r.lockedForUpdate;
        this.appendCounter = r.appendCounter;
        this.removalInfo = null;
    }

    RootReference updateRootPage(Page page, long attemptCounter, VisitablePages removedPositions) {
        return new RootReference(this, page, attemptCounter, removedPositions);
    }

    RootReference markLocked(int attemptCounter) {
        return new RootReference(this, attemptCounter);
    }

    RootReference unlockAndUpdateVersion(RootReference previous, long version, int attempt) {
        if (previous.lockedForUpdate) {
            previous = new RootReference(previous, previous.root, previous.getAppendCounter(), false, null);
        }
        return new RootReference(this, previous, version, attempt);
    }

    RootReference updatePageAndLockedStatus(Page page, int appendCounter, boolean lockedForUpdate, VisitablePages removedPositions) {
        return new RootReference(this, page, appendCounter, lockedForUpdate, removedPositions);
    }

    void removeUnusedOldVersions(long oldest) {
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for(RootReference rootRef = this; rootRef != null; rootRef = rootRef.previous) {
            if (rootRef.version < oldest) {
                rootRef.previous = null;
            }
        }
    }

    long getVersion() {
        RootReference prev = this.previous;
        return prev == null || prev.root != root ||
                prev.appendCounter != appendCounter ?
                    version : prev.version;
    }

    RemovalInfoNode extractRemovalInfo() {
        RemovalInfoNode result = removalInfo;
        removalInfo = null;
        return result;
    }

    void updateRemovalInfoData(VisitablePages data) {
        RemovalInfoNode ri = removalInfo;
        if (ri != null) {
//            if (data != null) {
                ri.data = data;
//            } else {
//                removalInfo = removalInfo.next;
//            }
        }
    }

    int getAppendCounter() {
        return appendCounter & 0xff;
    }

    public long getTotalCount() {
        return root.getTotalCount() + getAppendCounter();
    }

    @Override
    public String toString() {
        return "RootReference{" + System.identityHashCode(root) + "," + version + "," + lockedForUpdate +
                "," + getAppendCounter() + ", " + (removalInfo == null ? "null" : "rinf") + "}";
    }

    public interface VisitablePages {
        /**
         * Arrange for a specified visitor to visit every page in a subtree rooted at this page.
         * @param visitor to visit pages
         */
        void visitPages(Page.Visitor visitor);
    }

    static final class RemovalInfoNode
    {
        public volatile VisitablePages data;
        public final RemovalInfoNode next;

        RemovalInfoNode(VisitablePages data, RemovalInfoNode next) {
            this.data = data;
            this.next = next;
        }
    }

    static final class RemovalInfo implements VisitablePages
    {
        private final long[] pagePositions;

        RemovalInfo(long[] positions) {
            pagePositions = positions;
        }

        @Override
        public void visitPages(Page.Visitor visitor) {
            for (long pagePos : pagePositions) {
                visitor.visit(null, pagePos);
            }
        }
    }
}
