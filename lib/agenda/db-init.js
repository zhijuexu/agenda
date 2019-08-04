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
    try {
      let res = await self._collection.createIndex(self._indicesLock, {name: 'findAndLockIndex'});
      console.log(res);
      res = await self._collection.createIndex(self._indicesSort, {name: 'findAndSortIndex'});
      console.log(res);
    } catch (e) {
      if (e) {
        debug('index creation failed');
        self.emit('error', e);
      } else {
        debug('index creation success');
        self.emit('ready');
      }

      if (cb) {
        cb(e, self._collection);
      }
    }
  })();
};
