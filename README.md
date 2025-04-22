# pgXRay - Advanced PostgreSQL Audit Tool

![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![License](https://img.shields.io/badge/License-CC_BY_4.0-lightgrey.svg)
![Version](https://img.shields.io/badge/Version-1.3.0-blue.svg)

pgXRay is a powerful tool for comprehensive auditing and documentation of PostgreSQL databases. This project was created to automate the process of auditing database structures and generating documentation, including visual ER diagrams.

## 🚀 Features

- **Complete Database Structure Audit**:
  - Tables, columns, data types
  - Indexes and constraints
  - Foreign keys and relationships
  
- **Data Analysis**:
  - Sample data from each table (up to SAMPLE_LIMIT rows)
  - Row count estimates and table size information
  
- **Code Examination**:
  - Complete function definitions
  - Triggers and their implementations
  
- **Visualization**:
  - ER diagrams in DOT and PNG formats
  - HTML tables in diagram nodes
  - XLabel edge annotations for relationships
  
- **Reporting**:
  - Detailed Markdown report
  - Execution process logging

## 📋 Requirements

- Python 3.9+
- psycopg2-binary >= 2.9
- graphviz >= 0.20
- `dot` utility (part of the Graphviz package)

## ⚙️ Installation

```bash
# Clone the repository
git clone https://github.com/T-6891/pgXRay.git
cd pgXRay

# Set up Python virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows
# venv\Scripts\activate
# On macOS/Linux
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Graphviz (Linux)
sudo apt install graphviz

# Install Graphviz (macOS)
brew install graphviz

# Install Graphviz (Windows)
# Download and install from https://graphviz.org/download/
```

## 🔧 Usage

### Basic Audit

```bash
python pgXRay.py --conn "postgresql://user:password@host:port/database" --md "audit_report.md"
```

### Full Parameter Set

```bash
python pgXRay.py --conn "postgresql://user:password@host:port/database" \
                 --md "audit_report.md" \
                 --dot "er_diagram.dot" \
                 --png "er_diagram.png"
```

### Command Line Parameters

| Parameter | Description | Default |
|---------|------------|---------|
| `--conn` | PostgreSQL connection string | (required) |
| `--md` | Path to save the Markdown report | `audit_report.md` |
| `--dot` | Path to save the DOT diagram file | `er_diagram.dot` |
| `--png` | Path to save the PNG diagram | `er_diagram.png` |

## 📊 Audit Results

Running the script will generate:

1. **ER diagram** in DOT and PNG formats visualizing table relationships
2. **Markdown report** including:
   - General database information (PostgreSQL version, DB size)
   - Table structures with data types
   - Sample data from each table
   - Complete function definitions
   - Trigger definitions

## 🛠️ Configuration

Main settings are located at the top of the script:

```python
# =====================
# Configuration
# =====================
SAMPLE_LIMIT      = 10                  # number of rows to sample
DOT_FILE          = 'er_diagram.dot'    # .dot file name
PNG_FILE          = 'er_diagram.png'    # .png file name
DEFAULT_MD_REPORT = 'audit_report.md'
# =====================
```

## 🤝 Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0).
For more details: [https://creativecommons.org/licenses/by/4.0/](https://creativecommons.org/licenses/by/4.0/)

## 👤 Author

**Vladimir Smelnitskiy**  
E-mail: master@t-brain.ru

## 📌 Roadmap

- [ ] Analysis of views and materialized views
- [ ] HTML export format
- [ ] Interactive diagrams
- [ ] Database structure comparison
- [ ] Optimization for large databases
