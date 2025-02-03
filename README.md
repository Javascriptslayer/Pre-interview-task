# Pre-interview-task
This repo is to help new joiner and candidates to get experience how take home coding task is accomplished 

# Sales Data Pipeline 📊

A simple ETL (Extract, Transform, Load) pipeline that combines sales data with user information and weather conditions, perfect for learning data engineering basics!


# 📊 Sales Data Analysis System

_A complete solution for merging sales records with customer data and generating business insights_ 🚀

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![Pandas](https://img.shields.io/badge/pandas-1.0%2B-orange)
![SQLite](https://img.shields.io/badge/SQLite-3.0%2B-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

## 🌟 Features

- 📥 **Data Integration**: Merge CSV sales data with API-sourced customer profiles
- 🔄 **Smart Merging**: Automatic ID matching between sales and user records
- 📈 **Business Analytics**:
  - Monthly/quarterly sales trends 📅
  - Top customer/product rankings 🏆
  - Average order value calculations 💰
- 💾 **Secure Storage**: SQLite database for persistent results

## 🛠 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/sales-analysis.git
   cd sales-analysis

2. 🔍 Database Exploration
Install DB Browser for SQLite:

📥 Download from sqlitebrowser.org

Available for Windows/macOS/Linux
1. Launch DB Browser
2. Click 'Open Database'
3. Select sales_data.db

Explore Data:
- Click 'Browse Data' tab to view records

📂 Project Structure

sales-analysis/
├── 📄 sales_analysis.py        # Main analysis script
├── 📄 test_sales_analysis.py   # Unit tests (pytest)
├── 📁 test_data/               # Sample datasets
│   ├── 📄 sales_data.csv       # Input template
│   └── 📄 test_users.json      # Mock API response
├─ 
└── 📄 README.md                # This documentation

🧪 Testing
pytest test_sales_analysis.py -v
