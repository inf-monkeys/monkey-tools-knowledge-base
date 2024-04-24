# Monkey Tools for Knowledge Base

[![Release Notes](https://img.shields.io/github/release/inf-monkeys/monkey-tools-knowledge-base)](https://github.com/inf-monkeys/monkey-tools-knowledge-base/releases)
[![GitHub star chart](https://img.shields.io/github/stars/inf-monkeys/monkey-tools-knowledge-base?style=social)](https://star-history.com/#inf-monkeys/monkey-tools-knowledge-base)
[![GitHub fork](https://img.shields.io/github/forks/inf-monkeys/monkey-tools-knowledge-base?style=social)](https://github.com/inf-monkeys/monkey-tools-knowledge-base/fork)

English | [中文](./README-ZH.md)

## 👨‍💻 Developers

This project has 2 main entrypoints:

-   `app.py`: Python Flask API Server
-   `worker.py`: Consume tasks like import vector from file.

### Prerequisite

-   Install Python 3.10 +
-   Vector Store: Currently elasticsearch8 and milvus is supported.


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

    You can now access the app on [http://localhost:8899](http://localhost:8899)

6. Start the worker:

    ```bash
    python worker.py
    ```
