# Blog Post Vectorization

This script (`vectorize_blog_posts.py`) generates vector embeddings for blog posts that don't have them yet.

## Prerequisites

1. **Azure OpenAI Resource**: You need an Azure OpenAI service with an embedding model deployment
2. **Environment Variables**: Set the following in your environment or `setup_env.sh`:
   - `AZURE_OPENAI_ENDPOINT`: Your Azure OpenAI endpoint (e.g., `https://your-resource.openai.azure.com/`)
   - `AZURE_OPENAI_API_KEY`: Your Azure OpenAI API key
   - `AZURE_OPENAI_EMBEDDING_DEPLOYMENT`: Deployment name for embeddings (default: `text-embedding-3-small`)
   - `SQLSERVER_CONN`: SQL Server connection string (already configured)

3. **Python Package**: Install the OpenAI package:
   ```bash
   pip install openai>=1.0.0
   ```

## How It Works

1. **Finds posts** with `blog_vector IS NULL`
2. **Combines** title, categories, and summary into a single text
3. **Generates embeddings** using Azure OpenAI's embedding model (1536 dimensions)
4. **Updates** the `blog_vector` column in the database

## Usage

```bash
# Source environment variables
source setup_env.sh

# Run the vectorization script
python vectorize_blog_posts.py
```

## Embedding Model

The script uses Azure OpenAI's `text-embedding-3-small` model by default, which:
- Produces 1536-dimensional vectors
- Supports up to 8,191 tokens per input
- Is optimized for semantic search and similarity

## Text Format

The script combines post data in this format:
```
Title: [blog title] | Categories: [categories] | Summary: [summary]
```

## Error Handling

- Skips posts with no content
- Logs failures but continues processing
- Provides a summary report at the end

## Example Output

```
2025-12-17 12:00:00 - INFO - Found 10 posts without vectors
2025-12-17 12:00:01 - INFO - Processing post 1/10: My Blog Title
2025-12-17 12:00:02 - INFO - ✓ Successfully vectorized post ID 123
...
============================================================
Vectorization complete!
Successfully vectorized: 10
Failed: 0
Total processed: 10
============================================================
```
