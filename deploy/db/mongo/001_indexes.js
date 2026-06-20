// Run against the pylon apps database, e.g.:  mongosh "<uri>" deploy/db/mongo/001_indexes.js
db.apps.createIndex({ id: 1 }, { unique: true });
db.apps.createIndex({ key: 1 }, { unique: true });
