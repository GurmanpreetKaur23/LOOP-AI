const { getNextBatch, updateBatchStatus } = require('./storage');

function simulateExternalAPI(id) {
  return new Promise(resolve =>
    setTimeout(() => resolve({ id, data: 'processed' }), 1000)
  );
}

async function processBatch(batch) {
  updateBatchStatus(batch.batch_id, 'triggered');
  await Promise.all(batch.ids.map(simulateExternalAPI));
  updateBatchStatus(batch.batch_id, 'completed');
}

function processQueue() {
  setInterval(async () => {
    const batch = getNextBatch();
    if (batch) {
      console.log(`Processing batch: ${batch.batch_id}`);
      await processBatch(batch);
    }
  }, 5000); // One batch per 5 seconds
}

module.exports = { processQueue };
