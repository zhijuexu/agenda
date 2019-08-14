'use strict';
const debug = require('debug')('agenda:db_init');

/**
 * Setup and initialize the collection used to manage Jobs.
 * @name Agenda#dbInit
 * @function
 * @param {String} collection name or undefined for default 'agendaJobs'
 * @param {Function} cb called when the db is initialized
 * @returns {undefined}
 */
module.exports = function (collection, cb) {
  const self = this;
  debug('init database collection using name [%s]', collection);
  this._collection = this._mdb.collection(collection || 'agendaJobs');

  (async function () {
    debug('attempting index creation');

    let error;
    try {
      // todo@xm remove index 'findAndLockNextJobIndex'
      await self._collection.createIndex(self._indicesLock, {name: 'findAndLockIndex'});
      await self._collection.createIndex(self._indicesSort, {name: 'findAndSortIndex'});
    } catch (e) {
      error = e;
    }

    if (error) {
      debug('index creation failed');
      self.emit('error', error);
    } else {
      debug('index creation success');
      self.emit('ready');
    }

    if (cb) {
      cb(error, self._collection);
    }
  })();
};
