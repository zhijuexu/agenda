'use strict';
const debug = require('debug')('agenda:internal:_findAndLockNextJob');
const utils = require('../utils');
const uuid = require('uuid/v4');

const createJob = utils.createJob;

/**
 * Find and lock jobs
 * @name Agenda#findAndLockNextJob
 * @function
 * @param {String} jobName name of job to try to lock
 * @param {Object} definition definition used to tell how job is run
 * @param {Function} cb called when job lock fails or passes
 * @access protected
 * @caller jobQueueFilling() only
 * @returns {undefined}
 */
module.exports = function (jobName, definition, cb) {
  const self = this;
  const now = new Date();
  const lockDeadline = new Date(Date.now().valueOf() - definition.lockLifetime);
  const lockedUuid = uuid();
  debug('_findAndLockNextJob(%s, [Function], cb)', jobName);

  // Don't try and access MongoDB if we've lost connection to it.
  // Trying to resolve crash on Dev PC when it resumes from sleep. NOTE: Does this still happen?
  const s = this._mdb.s || this._mdb.db.s;
  if (s.topology.connections().length === 0) {
    if (s.topology.autoReconnect && !s.topology.isDestroyed()) {
      // Continue processing but notify that Agenda has lost the connection
      debug('Missing MongoDB connection, not attempting to find and lock a job');
      self.emit('error', new Error('Lost MongoDB connection'));
      cb();
    } else {
      // No longer recoverable
      debug('topology.autoReconnect: %s, topology.isDestroyed(): %s', s.topology.autoReconnect, s.topology.isDestroyed());
      cb(new Error('MongoDB connection is not recoverable, application restart required'));
    }
  } else {
    // /**
    // * Query used to find job to run
    // * @type {{$or: [*]}}
    // */
    const JOB_PROCESS_WHERE_QUERY = {
      $or: [{
        name: jobName,
        lockedAt: null,
        nextRunAt: {$lte: this._nextScanAt},
        disabled: {$ne: true}
      }, {
        name: jobName,
        lockedAt: {$exists: false},
        nextRunAt: {$lte: this._nextScanAt},
        disabled: {$ne: true}
      }, {
        name: jobName,
        lockedAt: {$lte: lockDeadline},
        disabled: {$ne: true}
      }]
    };

    /**
     * Query used to set a job as locked
     * @type {{$set: {lockedAt: Date}}}
     */
    const JOB_PROCESS_SET_QUERY = {$set: {lockedAt: now, lockedUuid}};

    (async function () {
      try {
        let res = await self._collection.updateMany(JOB_PROCESS_WHERE_QUERY, JOB_PROCESS_SET_QUERY);
        console.log('res.matchedCount', res.matchedCount);
        console.log('res.modifiedCount', res.modifiedCount);

        let jobs = await self._collection.find({lockedUuid}).sort(this._sort).toArray();
        console.log('find-and-lock-jobs', jobs);

        cb(null, jobs.map(job => createJob(self, job)));
      } catch (e) {
        console.error(e);
        debug('error occurred when running query to find and lock job');
        cb(e, null);
      }
    })();
  }
};
