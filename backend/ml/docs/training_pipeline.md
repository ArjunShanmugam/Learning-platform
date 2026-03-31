# Training Pipeline Documentation

## Overview
This document describes the machine learning training pipeline for the recommendation system. The pipeline trains item similarity models using user interaction data and provides tools for scheduling, reproducibility testing, and evaluation.

## Prerequisites
- Python 3.8+
- All dependencies installed from `requirements.txt`
- Access to training data in `data/raw/` (interactions.csv)
- Sufficient disk space for model artifacts (typically 500MB-2GB per training run)
- MySQL database connection for fetching interaction data
- Redis (optional, for caching during training)

## Dependencies
The training pipeline requires:
- `numpy` - Numerical computations
- `pandas` - Data processing
- `scikit-learn` - ML algorithms and metrics
- `sentence-transformers` - Semantic embeddings
- `faiss-cpu` - Efficient similarity search
- `schedule` - Job scheduling for automated training
- `sqlalchemy` - Database ORM
- `pymysql` - MySQL connector

Install all dependencies:
```bash
pip install -r requirements.txt
```

## Directory Structure
```
backend/
├── ml/
│   ├── scheduler.py           # Training job scheduler
│   ├── scripts/
│   │   ├── train_model.py     # Main training pipeline
│   │   ├── evaluate.py        # Model evaluation
│   │   └── predict.py         # Inference engine
│   └── docs/
│       └── training_pipeline.md
├── test_reproducibility.py    # Reproducibility tests
└── data/
    ├── raw/                   # Input data
    │   └── interactions.csv   # User interaction logs
    └── models/                # Trained models
        ├── run_1/
        ├── run_2/
        └── {timestamp}/       # Timestamped model directories
```

## Training Pipeline Architecture

### Pipeline Steps

1. **Data Loading & Preprocessing**
   - Load interactions data from `data/raw/interactions.csv`
   - Clean and validate data (remove duplicates, handle missing values)
   - Split data into training (80%) and test (20%) sets
   - Preserve temporal order for realistic evaluation

2. **Feature Engineering**
   - Extract user-course interaction patterns
   - Compute course metadata embeddings using sentence-transformers
   - Generate user preference vectors from interaction history
   - Create similarity matrices

3. **Model Training**
   - Train item similarity models (content-based + collaborative)
   - Use scikit-learn algorithms (cosine similarity, matrix factorization)
   - Optimize hyperparameters based on validation set
   - Save model artifacts (pickle format) to timestamped directory

4. **Model Evaluation**
   - Calculate metrics: Precision@K, Recall@K, NDCG, MRR
   - Generate confusion matrices and ROC curves
   - Compare predictions against test set
   - Log results for reproducibility tracking

5. **Model Export**
   - Save trained models to FAISS index for fast inference
   - Store metadata (training date, metrics, hyperparameters)
   - Create model version registry

### Training Script

Run a single training job manually:

```bash
cd backend
python -m ml.scripts.train_model \
    --input-file data/raw/interactions.csv \
    --output-dir data/models/manual_run \
    --test-size 0.2 \
    --random-state 42
```

**Arguments:**
- `--input-file` - Path to interaction data CSV
- `--output-dir` - Directory to save model artifacts
- `--test-size` - Fraction of data for testing (default: 0.2)
- `--random-state` - Seed for reproducibility (default: 42)

**Output Files:**
- `model.pkl` - Trained model object
- `metadata.json` - Training metadata (date, version, hyperparameters)
- `metrics.json` - Evaluation metrics
- `test_data.parquet` - Test set for evaluation
- `train_data.parquet` - Training set used

## Scheduled Training Jobs

### Running the Scheduler

The scheduler automatically runs training jobs on a defined schedule.

**Start the scheduler:**
```bash
cd backend
python -m ml.scheduler
```

**Default Schedule:**
- Daily training at 02:00 AM (UTC)
- Models stored in `data/models/{YYYYMMDD_HHMMSS}/`
- Logs written to `training_scheduler.log`

**Customizing the Schedule:**

Edit `backend/ml/scheduler.py` to modify the schedule:

```python
# Daily at specific time
schedule.every().day.at("02:00").do(run_training_job)

# Every N hours
schedule.every(6).hours.do(run_training_job)

# Every N days
schedule.every(7).days.do(run_training_job)

# Every Monday at 3:00 AM
schedule.every().monday.at("03:00").do(run_training_job)

# Multiple times per day
schedule.every().day.at("02:00").do(run_training_job)
schedule.every().day.at("14:00").do(run_training_job)
```

**Stopping the Scheduler:**
- Press `Ctrl+C` in the terminal
- The scheduler will log the interruption and exit gracefully

**Monitoring:**
- Check `training_scheduler.log` for execution logs
- Verify model artifacts in `data/models/`
- Use `flower` (if using Celery) to monitor background tasks

### Running in Background (Production)

**Option 1: Using systemd (Linux/Ubuntu)**
```bash
# Create service file
sudo nano /etc/systemd/system/training-scheduler.service

[Unit]
Description=Learning Platform Training Scheduler
After=network.target

[Service]
Type=simple
User=www-data
WorkingDirectory=/path/to/learning-platform/backend
ExecStart=/path/to/venv/bin/python -m ml.scheduler
Restart=always

[Install]
WantedBy=multi-user.target

# Enable and start
sudo systemctl enable training-scheduler.service
sudo systemctl start training-scheduler.service
sudo systemctl status training-scheduler.service
```

**Option 2: Using screen (Quick Development)**
```bash
cd backend
screen -S training-scheduler -d -m python -m ml.scheduler
# Detaches and runs in background
```

**Option 3: Using nohup (Simple)**
```bash
cd backend
nohup python -m ml.scheduler > training_scheduler.log 2>&1 &
```

## Testing Reproducibility

### Purpose
Verify that the training pipeline produces consistent results across multiple runs with fixed random seeds. This ensures model training is deterministic and reliable.

### Running Reproducibility Tests

```bash
cd backend
python test_reproducibility.py
```

**Test Configuration:**
- Runs 3 independent training cycles by default
- Each run uses fixed random seed (42)
- Compares metrics across all runs
- Detects high variance (std > 1e-6) and warns users

**Output Example:**
```
INFO:__main__:Starting training run 1/3
INFO:__main__:Starting training run 2/3
INFO:__main__:Starting training run 3/3
INFO:__main__:precision@10: Mean=0.7834, Std=0.000001
INFO:__main__:recall@10: Mean=0.4521, Std=0.000000
INFO:__main__:All runs: [0.7834, 0.7834, 0.7834]
```

### Customizing Reproducibility Tests

Edit `test_reproducibility.py`:

```python
# Change number of runs
test_reproducibility(num_runs=5)

# Use different data path
test_reproducibility(num_runs=3, data_path="data/custom/interactions.csv")

# Set variance threshold in run
if std_val > 0.001:  # Custom threshold
    logger.warning(f"High variance in {metric}")
```

### Expected Results
- **Identical Metrics**: All runs should produce identical (or near-identical) metric values
- **Zero Variance**: std_val should be < 1e-6 for deterministic training
- **Model Reproducibility**: Models trained in different runs should produce identical predictions on same data

## Model Evaluation Metrics

The pipeline computes the following metrics:

| Metric | Description | Range | Interpretation |
|--------|-------------|-------|-----------------|
| **Precision@K** | Fraction of top-K recommendations that are relevant | 0-1 | Higher is better; how many recommendations were correct |
| **Recall@K** | Fraction of relevant items in top-K | 0-1 | Higher is better; coverage of all relevant recommendations |
| **NDCG** | Normalized Discounted Cumulative Gain | 0-1 | Accounts for ranking order; higher is better |
| **MRR** | Mean Reciprocal Rank | 0-1 | Average of 1/rank for first relevant item; higher is better |

## Troubleshooting

### Issue: ModuleNotFoundError for training scripts
**Solution:**
```bash
# Ensure you're in backend directory
cd backend
export PYTHONPATH=$(pwd):$PYTHONPATH
python -m ml.scheduler
```

### Issue: Scheduler not running scheduled jobs
**Solution:**
1. Check logs: `tail -f training_scheduler.log`
2. Verify scheduler is running: `ps aux | grep scheduler`
3. Check system time is correct: `date`
4. Ensure write permissions in `data/models/` directory

### Issue: Out of memory during training
**Solution:**
- Reduce batch size in `train_model.py`
- Use fewer interactions in input data
- Use `faiss-gpu` instead of `faiss-cpu` if GPU available
- Increase system swap space

### Issue: Model files not being saved
**Solution:**
1. Check directory permissions: `ls -la data/models/`
2. Ensure sufficient disk space: `df -h`
3. Check write permissions: `touch data/models/test.txt`
4. Review error logs in `training_scheduler.log`

### Issue: Reproducibility test shows high variance
**Cause**: Non-deterministic operations or missing random seed initialization
**Solution**:
1. Verify `random_state=42` passed to all sklearn functions
2. Check numpy seed is set: `np.random.seed(42)`
3. Review training script for any random operations without seeds
4. Check TensorFlow/PyTorch seeds if using neural networks

## Performance Optimization

### Strategies to Speed Up Training

1. **Reduce Data Size**
   - Sample interactions: `df.sample(frac=0.5)`
   - Filter old data: `df[df['timestamp'] > cutoff_date]`

2. **Parallelize Processing**
   ```python
   from joblib import Parallel, delayed
   results = Parallel(n_jobs=-1)(
       delayed(process_chunk)(chunk) for chunk in chunks
   )
   ```

3. **Use GPU Acceleration**
   - Install `faiss-gpu`: `pip install faiss-gpu`
   - Install `cupy` for GPU-accelerated numpy operations

4. **Optimize Database Queries**
   - Add indexes on frequently queried columns
   - Use connection pooling
   - Cache frequently accessed data

5. **Model Checkpointing**
   - Save intermediate models during training
   - Resume from checkpoint if training interrupted
   - Reduces retraining time for long-running jobs

## Best Practices

1. **Always Set Random Seeds**
   ```python
   np.random.seed(42)
   random.seed(42)
   torch.manual_seed(42)
   ```

2. **Monitor Training Progress**
   - Log metrics at regular intervals
   - Plot loss/accuracy over time
   - Alert on anomalies (sudden drops in performance)

3. **Version Your Models**
   - Use timestamps for automatic versioning
   - Store git commit hash with model metadata
   - Maintain model changelog

4. **Validate Before Deployment**
   - Always run reproducibility tests
   - Compare new model performance against baseline
   - A/B test new models in production

5. **Maintain Data Quality**
   - Regularly audit interaction data
   - Remove outliers and errors
   - Document data schema changes

6. **Backup Important Models**
   - Keep multiple model versions
   - Store in version control or artifact storage
   - Document model lineage and creation date

## Advanced: Integrating with Celery

For distributed training across multiple machines, integrate with Celery:

```python
# tasks.py
from celery import Celery
from ml.scripts.train_model import train_item_similarity_model

app = Celery('learning_platform')

@app.task
def scheduled_training_job():
    train_item_similarity_model(
        input_file="data/raw/interactions.csv",
        output_dir=f"data/models/{datetime.now().isoformat()}"
    )

# In main.py
from celery.schedules import crontab
app.conf.beat_schedule = {
    'train-daily': {
        'task': 'tasks.scheduled_training_job',
        'schedule': crontab(hour=2, minute=0),  # 2 AM daily
    },
}
```

## Support & Contact

For issues or questions:
1. Check logs: `training_scheduler.log`
2. Review this documentation
3. Check git commit history for recent changes
4. Contact development team with error logs and system info
