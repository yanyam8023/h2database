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
     * Counter of reenterant locks.
     */
    private final byte holdCount;
    /**
     * Lock owner thread id.
     */
    private final long ownerId;
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
        this.holdCount = 0;
        this.ownerId = 0;
        this.appendCounter = 0;
        this.removalInfo = null;
    }

    private RootReference(RootReference r, Page root, long updateAttemptCounter, VisitablePages removedPositions) {
        this.root = root;
        this.version = r.version;
        this.previous = r.previous;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + updateAttemptCounter;
        this.holdCount = 0;
        this.ownerId = 0;
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
        assert r.holdCount == 0 || r.ownerId == Thread.currentThread().getId() : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount + 1);
        this.ownerId = Thread.currentThread().getId();
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
        assert r.holdCount > 0 && r.ownerId == Thread.currentThread().getId() : Thread.currentThread().getId() + " " + r;
        this.holdCount = (byte)(r.holdCount - (lockedForUpdate ? 0 : 1));
        this.ownerId = this.holdCount == 0 ? 0 : Thread.currentThread().getId();
        this.appendCounter = (byte) appendCounter;
        this.removalInfo = removedPositions == null ? r.removalInfo :
                                                    new RemovalInfoNode(removedPositions, r.removalInfo);
    }

    // This one is used for version change
    private RootReference(RootReference r, long version, int attempt) {
        this.root = r.root;
        this.version = version;
        this.previous = r;
        this.updateCounter = r.updateCounter + 1;
        this.updateAttemptCounter = r.updateAttemptCounter + attempt;
        this.holdCount = 0;
        this.ownerId = 0;
        assert r.appendCounter == 0;
        this.appendCounter = 0;
        this.removalInfo = null;
    }

    RootReference updateRootPage(Page page, long attemptCounter, VisitablePages removedPositions) {
        if (holdCount == 0) {
            RootReference updatedRootReference = new RootReference(this, page, attemptCounter, removedPositions);
            if (page.map.compareAndSetRoot(this, updatedRootReference)) {
                return updatedRootReference;
            }
        }
        return null;
    }

    RootReference tryLock(int attemptCounter) {
        if (holdCount == 0 || ownerId == Thread.currentThread().getId()) {
            RootReference lockedRootReference = new RootReference(this, attemptCounter);
            if (root.map.compareAndSetRoot(this, lockedRootReference)) {
                return lockedRootReference;
            }
        }
        return null;
    }

    RootReference unlockAndUpdateVersion(long version, int attempt) {
        assert holdCount == 0 || ownerId == Thread.currentThread().getId() : Thread.currentThread().getId() + " " + this;
        RootReference previous = this;
        RootReference tmp;
        while ((tmp = previous.previous) != null && tmp.root == root) {
            previous = tmp;
        }
        RootReference updatedRootReference = new RootReference(previous, version, attempt);
        if (root.map.compareAndSetRoot(this, updatedRootReference)) {
            return updatedRootReference;
        }
        return null;
    }

    RootReference updatePageAndLockedStatus(Page page, int appendCounter, boolean lockedForUpdate, VisitablePages removedPositions) {
        return new RootReference(this, page, appendCounter, lockedForUpdate, removedPositions);
    }

    void removeUnusedOldVersions(long oldestVersionToKeep) {
        // We need to keep at least one previous version (if any) here,
        // because in order to retain whole history of some version
        // we really need last root of the previous version.
        // Root labeled with version "X" is the LAST known root for that version
        // and therefore the FIRST known root for the version "X+1"
        for(RootReference rootRef = this; rootRef != null; rootRef = rootRef.previous) {
            if (rootRef.version < oldestVersionToKeep) {
                RootReference previous;
                assert (previous = rootRef.previous) == null || previous.getAppendCounter() == 0 : oldestVersionToKeep + " " + rootRef.previous;
                rootRef.previous = null;
            }
        }
    }

    boolean isLocked() {
        return holdCount != 0;
    }

    long getVersion() {
        RootReference prev = previous;
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
        return "RootReference{" + System.identityHashCode(root) + "," + version + "," + ownerId + ":" + holdCount +
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
