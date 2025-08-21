# 🎬 ETL Pipeline for IMDb Data using Python and MySQL

## 📌 Project Overview

A Python-based ETL (Extract, Transform, Load) project to process the IMDb dataset and load it into a MySQL database. This pipeline is designed to be robust and handle large datasets in memory-efficient chunks.

---

## 🛠️ Tech Stack

- **Python**
- **Pandas**
- **MySQL / mysql-connector-python**

---

## 🚀 Getting Started

*Follow these steps to set up and run the project on your local machine.*

### Prerequisites

- **Python 3.x**: Ensure you have a recent version of Python installed.

- **MySQL Server**: A running MySQL instance is required to host the database.

- **IMDb Dataset**: The project will automatically download the necessary datasets (title.basics.tsv.gz, title.ratings.tsv.gz, name.basics.tsv.gz).

---

1. **Clone the repository**
    ```bash
    git clone https://github.com/Gireeshs02/imdb-data-pipeline.git
    cd imdb-data-pipeline
    ```
2. **Set up a Virtual Environment**
    ```bash
    py -m venv venv
    # On Windows
    venv\Scripts\activate
    # On macOS/Linux
    source venv/bin/activate
    ```

3. **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4. **Configure ```.env``` file with your MySQL credentials**
    ```bash
    DB_USER = user_name 
    DB_PASSWORD = your_password
    DB_HOST = localhost
    DB_PORT = port_number
    DB_NAME = database_name
    ```

5. **Set up *MySQL database***
    - Run the schema file inside MySQL:
    ```bash
    mysql -u root -p your_database_name < schema.sql
    ```

6. **Download the Data**
    ```bash
    py download_data.py
    ```

7. **Run the Project**
    ```bash
    py main.py
    ```

---

## 🤝 Contributing

Contributions are welcome!
Feel free to open issues, submit pull requests, or suggest improvements.