 
const express = require('express');
// const bodyParser = require('body-parser');
const { createIngestion, getStatus } = require('./storage');
const { processQueue } = require('./processor');

const app = express();
app.use(express.json());
// app.use(bodyParser.json()) ;

app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;

  if (!ids || !Array.isArray(ids) || !priority) {
    return res.status(400).json({ error: 'Invalid payload' });
  }

  const ingestion_id = createIngestion(ids, priority);
  return res.json({ ingestion_id });
});

app.get('/status/:ingestion_id', (req, res) => {
  const status = getStatus(req.params.ingestion_id);
  if (!status) return res.status(404).json({ error: 'Not found' });
  return res.json(status);
});

app.get('/', (req, res) => {
  res.json({ message: 'Server is running' });
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
  processQueue(); 
});
