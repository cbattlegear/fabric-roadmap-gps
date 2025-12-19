# Semantic Matching: Connecting Roadmap Releases to Blog Articles

## Overview

Fabric GPS uses Azure OpenAI embeddings and vector similarity search to automatically discover relevant blog articles for each roadmap item. This creates a semantic bridge between the official Microsoft Fabric Roadmap and the Microsoft Fabric Blog, helping users find related reading material without manual curation.

## The Challenge

Microsoft publishes roadmap items with technical descriptions and separate blog articles with in-depth explanations. Traditionally, connecting these would require:
- Manual tagging and categorization
- Keyword-based search (brittle and limited)
- Time-consuming human curation

Instead, we use semantic embeddings to understand the *meaning* of content and find natural connections.

## Other Potential Uses

This could be used as a simple method to replace a feature like Full Text indexes. You get the free form similarity search of full text indexing without the overhead. If you really want to keep this fully embedded into SQL you can also build this out using the REST call system in SQL Azure. This would let you make a full text similarity search in a single stored procedure.

## How It Works

### 1. Generate Embeddings

We use Azure OpenAI's `text-embedding-3-small` model to convert text into 1536-dimensional vectors. Each vector represents the semantic meaning of the content in a high-dimensional space.

```python
from openai import AzureOpenAI

client = AzureOpenAI(
    api_version="2024-02-01",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY")
)

def generate_embedding(text):
    response = client.embeddings.create(
        input=text,
        model=deployment_name
    )
    return response.data[0].embedding
```

### 2. Store Vectors in SQL Server

Azure SQL Database's native `vector(1536)` data type stores embeddings alongside the original content:

```sql
-- Blog posts table with embeddings
CREATE TABLE fabric_blog_posts (
    url NVARCHAR(500) PRIMARY KEY,
    title NVARCHAR(500),
    summary NVARCHAR(MAX),
    blog_vector vector(1536),  -- Semantic embedding
    ...
);

-- Release items table with embeddings
ALTER TABLE release_items
ADD release_vector vector(1536);
```

### 3. Find Similar Content with Cosine Distance

SQL Server's `VECTOR_DISTANCE('cosine', ...)` function calculates semantic similarity:

```sql
SELECT TOP 1 
    url, 
    title,
    VECTOR_DISTANCE('cosine', release_vector, blog_vector) AS distance
FROM fabric_blog_posts
WHERE blog_vector IS NOT NULL
ORDER BY distance ASC;
```

**Cosine distance** measures the angle between two vectors:
- `0.0` = identical meaning
- `1.0` = completely unrelated
- Lower values = more similar content

### 4. Update Release Items with Matches

Once we find the most related blog article, we store it directly on the release item:

```sql
UPDATE release_items
SET 
    blog_title = @blog_title,
    blog_url = @blog_url
WHERE id = @release_id;
```

## The Pipeline

### Step 1: Scrape Blog Posts
```bash
python scrape_fabric_blog.py --rss
```
- Reads the Microsoft Fabric Blog RSS feed
- Stores new articles in `fabric_blog_posts` table
- Extracts: URL, title, summary, author, categories

### Step 2: Vectorize Blog Posts
```bash
python vectorize_blog_posts.py
```
- Finds blog posts without embeddings
- Generates embeddings for title + summary
- Stores vectors in `blog_vector` column

### Step 3: Vectorize Releases
```bash
python match_releases_to_blogs.py
```
- Finds releases without embeddings
- Generates embeddings from release descriptions
- Stores vectors in `release_vector` column
- **Finds most similar blog article** using cosine distance
- Updates `blog_title` and `blog_url` on the release

### Step 4: Display in UI

The web interface shows matched blog articles in the release detail modal:

```javascript
if (item.blog_title && item.blog_url) {
    return `
        <div class="blog-related-section">
            <h4>📚 Related Blog Article</h4>
            <div class="blog-article-card">
                <a href="${escapeHtml(item.blog_url)}" 
                   target="_blank" 
                   class="blog-article-link">
                    <div class="blog-article-title">
                        ${escapeHtml(item.blog_title)}
                    </div>
                </a>
            </div>
        </div>
    `;
}
```

## Automatic Re-processing

When release content changes, we automatically null the vectors to trigger re-processing:

```python
if existing_item.description != item.description:
    existing_item.release_vector = None
    existing_item.blog_title = None
    existing_item.blog_url = None
```

This ensures matches stay current as roadmap items evolve.

## Why This Approach Works

### Advantages
- **No manual curation**: Fully automated matching
- **Semantic understanding**: Captures meaning, not just keywords
- **Self-healing**: Automatically updates when content changes
- **Scalable**: Works for hundreds of releases and blog posts
- **Transparent**: Distance scores show confidence level

### Limitations
- **Creative connections**: Sometimes matches tangentially related content
- **Context blind**: Doesn't understand time/release windows
- **Embedding quality**: Limited by the underlying model
- **One match only**: Currently shows single "best" match

## Technical Specifications

| Component | Technology | Details |
|-----------|-----------|---------|
| Embedding Model | Azure OpenAI `text-embedding-3-small` | 1536 dimensions |
| Vector Storage | Azure SQL Database | `vector(1536)` data type |
| Similarity Metric | Cosine Distance | `VECTOR_DISTANCE('cosine', v1, v2)` |
| API Version | `2024-02-01` | Azure OpenAI API |
| Update Frequency | Daily | Runs with fetch jobs |


## Cost Considerations

Azure OpenAI embedding costs are minimal:
- `text-embedding-3-small`: ~$0.02 per 1M tokens
- Average blog post: ~500 tokens
- Average release: ~200 tokens
- **Total for 100 releases + 100 blogs**: ~$0.001

The vector storage in SQL Server adds negligible overhead (6KB per 1536-dimensional vector).

---

**Questions or improvements?** Open an issue on [GitHub](https://github.com/cbattlegear/fabric-roadmap-gps/issues) or reach out at [cameron@fabric-gps.com](mailto:cameron@fabric-gps.com).
