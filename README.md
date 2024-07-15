# Monkey Tools for Knowledge Base

[![Release Notes](https://img.shields.io/github/release/inf-monkeys/monkey-tools-knowledge-base)](https://github.com/inf-monkeys/monkey-tools-knowledge-base/releases)
[![GitHub star chart](https://img.shields.io/github/stars/inf-monkeys/monkey-tools-knowledge-base?style=social)](https://star-history.com/#inf-monkeys/monkey-tools-knowledge-base)
[![GitHub fork](https://img.shields.io/github/forks/inf-monkeys/monkey-tools-knowledge-base?style=social)](https://github.com/inf-monkeys/monkey-tools-knowledge-base/fork)

English | [中文](./README-ZH.md)

## 🐳 Docker 

We provide a helm chart, read more [here](https://github.com/inf-monkeys/monkeys-cloud/blob/main/helm/charts/community/tools/monkey-tools-knowledge-base/README.md).

## 👨‍💻 Developers

This project has 2 main entrypoints:

-   `app.py`: Python Flask API Server
-   `worker.py`: Consume tasks like import vector from file.

### Prerequisite

#### Vector Store

-   Install Python 3.10 +
-   Vector Store: Currently The following vector store is supported:
  
| Vector Store Type | Supported | Vector Search | Full Text Search |
| ----------------- | --------- | ------------- | ---------------- |
| `elasticsearch8`  | `Yes`     | `Yes`         | `Yes`            |
| `pgvector`        | `Yes`     | `Yes`         | `Yes`            |


</details>

<details>
<summary><kbd>PgVector Installation Guide</kbd></summary>

For most simple case, you can install PgVector using Docker:

```sh
docker run --name pgvector -p 5433:5432 -e POSTGRES_USER=postgres -e POSTGRES_DB=knowledge-base -e POSTGRES_PASSWORD=postgres -d pgvector/pgvector:pg16
```

And then run `CREATE EXTENSION vector;` inside postgres shell.

Or you can read [official doc](https://github.com/pgvector/pgvector/).

</details>


#### Download Embedding Models

This project uses [FlagEmbedding](https://github.com/FlagOpen/FlagEmbedding) to generate embeddings, if you have internet connection, it will download embedding models from huggingface automaticly. If not, or you want to reduce time when first download model, you can download the models mannualy.

Below are some models you can choose, or you can download any model you want from huggingface.

- BAAI/bge-base-zh-v1.5: 
    - huggingface repo: https://huggingface.co/BAAI/bge-base-zh-v1.5
    - CDN: https://static.infmonkeys.com/models/embeddings/bge-base-zh-v1.5.tar.gz

- jinaai/jina-embeddings-v2-base-en:
    - huggingface repo: https://huggingface.co/jinaai/jina-embeddings-v2-base-en
    - CDN: https://static.infmonkeys.com/models/embeddings/jina-embeddings-v2-base-en.tar.gz

- jinaai/jina-embeddings-v2-small-en:
    - huggingface: https://huggingface.co/jinaai/jina-embeddings-v2-small-en
    - CDN: https://static.infmonkeys.com/models/embeddings/jina-embeddings-v2-small-en.tar.gz

- moka-ai/m3e-base:
    - 描述: 适用于中文语料的 embedding
    - huggingface repo: https://huggingface.co/moka-ai/m3e-base
    - CDN 下载地址: https://static.infmonkeys.com/models/embeddings/jina-embeddings-v2-small-en.tar.gz

When download finished, You need to put it into `./models` folder.

### Configuration

Create a `config.yaml` in the source root directory: 

```sh
cp config.yaml.example config.yaml
```

### Setup


1. Clone the repository

    ```bash
    git clone https://github.com/inf-monkeys/monkey-tools-knowledge-base
    ```

2. Go into repository folder

    ```bash
    cd monkey-tools-knowledge-base
    ```

3. Install python dependencies:

    ```bash
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

4. Run db migrations:

    ```bash
    flask db upgrade
    ```

5. Start the API Server:

    ```bash
    python app.py
    ```

    You can now access the app on [http://localhost:5000](http://localhost:5000)

6. Start the worker:

    ```bash
    python worker.py
    ```
