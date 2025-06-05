const { v4: uuidv4 } = require('uuid');

const PRIORITY_ORDER = { HIGH: 1, MEDIUM: 2, LOW: 3 };

const ingestions = new Map();
const queue = [];

function createIngestion(ids, priority) {
  const ingestion_id = uuidv4();
  const created_time = Date.now();

  const batches = [];
  for (let i = 0; i < ids.length; i += 3) {
    const batch = {
      batch_id: uuidv4(),
      ids: ids.slice(i, i + 3),
      status: 'yet_to_start',
      created_time
    };
    batches.push(batch);
    queue.push({
      ingestion_id,
      priority,
      created_time,
      ...batch
    });
  }

  ingestions.set(ingestion_id, {
    ingestion_id,
    priority,
    created_time,
    status: 'yet_to_start',
    batches
  });

  return ingestion_id;
}

function getStatus(ingestion_id) {
  const record = ingestions.get(ingestion_id);
  if (!record) return null;

  const statuses = record.batches.map(b => b.status);
  let overall = 'yet_to_start';
  if (statuses.every(s => s === 'completed')) overall = 'completed';
  else if (statuses.some(s => s === 'triggered' || s === 'completed')) overall = 'triggered';

  return {
    ingestion_id,
    status: overall,
    batches: record.batches
  };
}

function updateBatchStatus(batch_id, status) {
  for (const [, record] of ingestions) {
    const batch = record.batches.find(b => b.batch_id === batch_id);
    if (batch) {
      batch.status = status;
      break;
    }
  }
}

function getNextBatch() {
  if (queue.length === 0) return null;

  // Sort by priority and created_time
  queue.sort((a, b) => {
    if (PRIORITY_ORDER[a.priority] !== PRIORITY_ORDER[b.priority]) {
      return PRIORITY_ORDER[a.priority] - PRIORITY_ORDER[b.priority];
    }
    return a.created_time - b.created_time;
  });

  return queue.shift();
}

module.exports = {
  createIngestion,
  getStatus,
  updateBatchStatus,
  getNextBatch
};
